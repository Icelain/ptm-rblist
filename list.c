#include <libpmemobj.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Pool layout declaration
POBJ_LAYOUT_BEGIN(rb_list_layout);
POBJ_LAYOUT_TOID(rb_list_layout, struct rb_node);
POBJ_LAYOUT_TOID(rb_list_layout, struct version_list);
POBJ_LAYOUT_ROOT(rb_list_layout, struct rb_root);
POBJ_LAYOUT_END(rb_list_layout);

// Color definitions (no longer used for RB tree properties, but type remains)
typedef enum { RED = 0, BLUE = 1 } rb_color_t;

// Operation types from MVOptm [6]
typedef enum { INSERT = 0, DELETE = 1, LOOKUP = 2 } op_name_t;

// Status types from MVOptm [6]
typedef enum { ABORT = 0, OK = 1, FAIL = 2, COMMIT = 3 } status_t;

// Node structure for lazyrb-list (Skip List) [1, 4]
struct rb_node {
  int key;                      // G_key
  TOID(struct version_list) vl; // G_vl - version list
  pthread_mutex_t lock;         // G_lock
  TOID(struct rb_node) RL;      // Red Link for lazyrb-list
  TOID(struct rb_node) BL;      // Blue Link for lazyrb-list
  bool marked;                  // Lazy deletion mark
};

// Version list structure (based on MVOptm G_vl) [4, 26]
struct version_list {
  int ts;      // G_ts - timestamp
  int val;     // G_val - value
  bool mark;   // G_mark - deletion mark for this specific version
  int rvl[30]; // G_rvl - return value list (fixed size for simplicity) [31]
  TOID(struct version_list) vnext; // G_vnext - next version
};

// Root structure for the persistent lazyrb-list [5]
struct rb_root {
  TOID(struct rb_node) head; // Sentinel head node (-infinity)
  TOID(struct rb_node) tail; // Sentinel tail node (+infinity)
  atomic_int global_counter; // G_cnt from MVOptm
  size_t node_count;
};

// Local transaction record (based on MVOptm L_rec) [32]
struct local_rec {
  int obj_id; // L_obj_id
  int key;    // L_key
  int val;    // L_val
  TOID(struct rb_node)
  preds[6]; // preds for BL predecessor, preds[8] for RL predecessor [7]
  TOID(struct rb_node)
  currs[6]; // currs[8] for BL current, currs for RL current [7]
  TOID(struct rb_node)
  node; // The actual node found/inserted (used internally for the node itself)
        // [32]
  status_t op_status; // L_op_status [32]
  op_name_t opn;      // L_opn [32]
};

// Local transaction log (based on MVOptm L_txlog) [32]
struct tx_log {
  int tid;                   // L_tid
  status_t tx_status;        // L_tx_status
  struct local_rec *records; // L_list
  size_t record_count;
  size_t record_capacity;
};

// Global variables
static PMEMobjpool *pop = NULL;
static __thread struct tx_log *local_txlog = NULL; // [33]

// Sentinel node keys
#define NEG_INF INT_MIN // For head node
#define POS_INF INT_MAX // For tail node

// List insertion types for list_ins [14] (derived from Algorithm 17)
typedef enum {
  LIST_TYPE_RL_ONLY,            // Insert new node into RL only
  LIST_TYPE_RL_BL,              // Insert new node into RL and BL
  LIST_TYPE_RL_BL_FROM_RL_TO_BL // Re-link an existing RL-only node into BL
} list_insert_type_t;

// Function prototypes
int ptm_init(void);
int ptm_begin(void);
int ptm_insert(int obj_id, int key, int val);
int ptm_delete(int obj_id, int key);
int ptm_lookup(int obj_id, int key, int *val, status_t *op_status);
int ptm_try_commit(void);
void ptm_abort(void);
void ptm_cleanup(void);

// Helper functions for local_txlog management
static struct local_rec *find_record(int obj_id, int key);
static int create_record(int obj_id, int key);
static void set_opn(int obj_id, int key, op_name_t op);
static void set_val(int obj_id, int key, int val);
static void set_op_status(int obj_id, int key, status_t status); // [34]
static void set_preds_currs(int obj_id, int key, TOID(struct rb_node) preds[],
                            TOID(struct rb_node)
                                currs[]); // Modified for arrays
static op_name_t get_opn(int obj_id, int key);
static int get_val(int obj_id, int key);
static status_t get_op_status(int obj_id, int key);
static void get_preds_currs(int obj_id, int key, TOID(struct rb_node) preds[],
                            TOID(struct rb_node)
                                currs[]); // Modified for arrays

// Lazyrb-list (Skip List) Internal Operations
static void acquire_pred_curr_locks(TOID(struct rb_node) preds[],
                                    TOID(struct rb_node) currs[]); // [20]
static void release_pred_curr_locks(TOID(struct rb_node) preds[],
                                    TOID(struct rb_node) currs[]); // [14]
static bool method_validation(TOID(struct rb_node) preds[],
                              TOID(struct rb_node) currs[]); // [21]
static void list_lookup_internal(int key, TOID(struct rb_node) * preds_out,
                                 TOID(struct rb_node) * currs_out); // [10]
static TOID(struct version_list)
    find_lts(int tid, TOID(struct rb_node) node);               // [27]
static bool check_versions(int tid, TOID(struct rb_node) node); // [24]
static TOID(struct rb_node)
    list_ins(TOID(struct rb_node) preds_RL, TOID(struct rb_node) curr_RL,
             TOID(struct rb_node) preds_BL, TOID(struct rb_node) curr_BL,
             TOID(struct rb_node) new_node,
             list_insert_type_t list_type); // [14]
static void list_del_bl(TOID(struct rb_node) preds_BL,
                        TOID(struct rb_node) currs_BL); // [18]
static void common_lu_del(int obj_id, int key, int *val,
                          status_t *op_status); // [11]
static void intra_trans_validation(struct local_rec *rec_current,
                                   TOID(struct rb_node) preds_current[],
                                   TOID(struct rb_node)
                                       currs_current[]); // [25]
static void release_ordered_locks(struct tx_log *log);   // [35]

// Actual operations (called during commit phase)
static int perform_insert(int key, int val);
static int perform_delete(int key);

// --- Helper function implementations for local_txlog ---
static struct local_rec *find_record(int obj_id, int key) {
  if (local_txlog == NULL)
    return NULL;
  for (size_t i = 0; i < local_txlog->record_count; i++) {
    if (local_txlog->records[i].obj_id == obj_id &&
        local_txlog->records[i].key == key) {
      return &local_txlog->records[i];
    }
  }
  return NULL;
}

static int create_record(int obj_id, int key) {
  if (local_txlog == NULL)
    return -1;
  if (local_txlog->record_count >= local_txlog->record_capacity) { // [36]
    local_txlog->record_capacity *= 2;
    local_txlog->records =
        realloc(local_txlog->records,
                sizeof(struct local_rec) * local_txlog->record_capacity);
    if (local_txlog->records == NULL) {
      perror("realloc failed");
      return -1;
    }
  }
  struct local_rec *rec = &local_txlog->records[local_txlog->record_count++];
  rec->obj_id = obj_id;
  rec->key = key;
  rec->val = 0;
  // Initialize preds/currs to NULL TOIDs
  rec->preds = TOID_NULL(struct rb_node);
  rec->preds[8] = TOID_NULL(struct rb_node);
  rec->currs = TOID_NULL(struct rb_node);
  rec->currs[8] = TOID_NULL(struct rb_node);
  rec->node = TOID_NULL(struct rb_node);
  rec->op_status = OK;
  rec->opn = LOOKUP; // Default operation [36]
  return 0;
}

static void set_opn(int obj_id, int key, op_name_t op) { // [37]
  struct local_rec *rec = find_record(obj_id, key);
  if (rec)
    rec->opn = op;
}

static void set_val(int obj_id, int key, int val) { // [37]
  struct local_rec *rec = find_record(obj_id, key);
  if (rec)
    rec->val = val;
}

static void set_op_status(int obj_id, int key, status_t status) { // [34]
  struct local_rec *rec = find_record(obj_id, key);
  if (rec)
    rec->op_status = status;
}

static void set_preds_currs(int obj_id, int key, TOID(struct rb_node) preds[],
                            TOID(struct rb_node) currs[]) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec) {
    rec->preds = preds;
    rec->preds[8] = preds[8];
    rec->currs = currs;
    rec->currs[8] = currs[8];
  }
}

static op_name_t get_opn(int obj_id, int key) { // [37]
  struct local_rec *rec = find_record(obj_id, key);
  return rec ? rec->opn : LOOKUP;
}

static int get_val(int obj_id, int key) { // [38]
  struct local_rec *rec = find_record(obj_id, key);
  return rec ? rec->val : 0;
}

static status_t get_op_status(int obj_id, int key) { // [38]
  struct local_rec *rec = find_record(obj_id, key);
  return rec ? rec->op_status : FAIL;
}

static void get_preds_currs(int obj_id, int key, TOID(struct rb_node) preds[],
                            TOID(struct rb_node) currs[]) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec) {
    preds = rec->preds;
    preds[8] = rec->preds[8];
    currs = rec->currs;
    currs[8] = rec->currs[8];
  } else { // Should ideally not happen if record exists
    preds = TOID_NULL(struct rb_node);
    preds[8] = TOID_NULL(struct rb_node);
    currs = TOID_NULL(struct rb_node);
    currs[8] = TOID_NULL(struct rb_node);
  }
}

// --- Lazyrb-list (Skip List) Internal Operations ---

// Acquire locks on the identified predecessor and current nodes [20]
static void acquire_pred_curr_locks(TOID(struct rb_node) preds[],
                                    TOID(struct rb_node) currs[]) {
  // Collect all distinct nodes (preds and currs) to lock, then sort by key for
  // order [19, 22]
  TOID(struct rb_node) nodes_to_lock[32];
  size_t count = 0;

  // Add unique non-NULL nodes to a temporary array
  if (!TOID_IS_NULL(preds))
    nodes_to_lock[count++] = preds;
  if (!TOID_IS_NULL(preds[8]) && !TOID_EQUALS(preds[8], preds))
    nodes_to_lock[count++] = preds[8];
  if (!TOID_IS_NULL(currs) && !TOID_EQUALS(currs, preds) &&
      !TOID_EQUALS(currs, preds[8]))
    nodes_to_lock[count++] = currs;
  if (!TOID_IS_NULL(currs[8]) && !TOID_EQUALS(currs[8], preds) &&
      !TOID_EQUALS(currs[8], preds[8]) && !TOID_EQUALS(currs[8], currs))
    nodes_to_lock[count++] = currs[8];

  // Simple bubble sort by key for distinct nodes to acquire locks in order
  for (size_t i = 0; i < count; ++i) {
    for (size_t j = i + 1; j < count; ++j) {
      // Compare by key to ensure increasing order
      if (D_RO(nodes_to_lock[i])->key > D_RO(nodes_to_lock[j])->key) {
        TOID(struct rb_node) temp = nodes_to_lock[i];
        nodes_to_lock[i] = nodes_to_lock[j];
        nodes_to_lock[j] = temp;
      }
    }
  }

  for (size_t i = 0; i < count; ++i) {
    pthread_mutex_lock(&D_RW(nodes_to_lock[i])->lock); // [20]
  }
}

// Release locks on the identified predecessor and current nodes [14]
static void release_pred_curr_locks(TOID(struct rb_node) preds[],
                                    TOID(struct rb_node) currs[]) {
  // Collect and sort distinct nodes, then release in the same order as acquired
  TOID(struct rb_node) nodes_to_unlock[32];
  size_t count = 0;

  if (!TOID_IS_NULL(preds))
    nodes_to_unlock[count++] = preds;
  if (!TOID_IS_NULL(preds[8]) && !TOID_EQUALS(preds[8], preds))
    nodes_to_unlock[count++] = preds[8];
  if (!TOID_IS_NULL(currs) && !TOID_EQUALS(currs, preds) &&
      !TOID_EQUALS(currs, preds[8]))
    nodes_to_unlock[count++] = currs;
  if (!TOID_IS_NULL(currs[8]) && !TOID_EQUALS(currs[8], preds) &&
      !TOID_EQUALS(currs[8], preds[8]) && !TOID_EQUALS(currs[8], currs))
    nodes_to_unlock[count++] = currs[8];

  // Simple bubble sort by key to release in acquired order
  for (size_t i = 0; i < count; ++i) {
    for (size_t j = i + 1; j < count; ++j) {
      if (D_RO(nodes_to_unlock[i])->key > D_RO(nodes_to_unlock[j])->key) {
        TOID(struct rb_node) temp = nodes_to_unlock[i];
        nodes_to_unlock[i] = nodes_to_unlock[j];
        nodes_to_unlock[j] = temp;
      }
    }
  }

  for (size_t i = 0; i < count; ++i) {
    pthread_mutex_unlock(&D_RW(nodes_to_unlock[i])->lock); // [14]
  }
}

// Method validation for RV methods and TryC methods [21]
static bool method_validation(TOID(struct rb_node) preds[],
                              TOID(struct rb_node) currs[]) {
  // [19] Validation checks:
  // (1) If pred and curr nodes of blue links are not marked
  // (2) If the next links of both blue and red pred nodes point to the correct
  // curr nodes
  if (D_RO(preds)->marked || D_RO(currs[8])->marked || // (1) for blue links
      !TOID_EQUALS(D_RO(preds)->BL, currs[8]) || // (2) for blue links [21]
      !TOID_EQUALS(D_RO(preds[8])->RL, currs)) { // (2) for red links [21]
    return false;
  }
  return true; // [21]
}

// Internal list lookup, identifies preds and currs for a key [10]
static void list_lookup_internal(int key, TOID(struct rb_node) * preds_out,
                                 TOID(struct rb_node) * currs_out) {
  TOID(struct rb_root) root_oid = POBJ_ROOT(pop, struct rb_root);
  TOID(struct rb_node)
  head_node = D_RO(root_oid)->head; // Get the list head [39]

  TOID(struct rb_node) p0 = head_node;    // preds - for BL
  TOID(struct rb_node) c1 = D_RO(p0)->BL; // currs[8] - for BL [39]

  // Search in BL (blue list) [39]
  while (D_RO(c1)->key < key) {
    p0 = c1;
    c1 = D_RO(p0)->BL;
  }

  TOID(struct rb_node) p1 = p0; // preds[8] - for RL, initially same as p0 [39]
  TOID(struct rb_node) c0 = D_RO(p1)->RL; // currs - for RL [39]

  // Search in RL (red list) [39]
  while (D_RO(c0)->key < key) {
    p1 = c0;
    c0 = D_RO(p1)->RL;
  }

  // Output results [7]
  preds_out = p0;
  preds_out[8] = p1;
  currs_out = c0;
  currs_out[8] = c1;
}

// Finds the version tuple with the largest timestamp smaller than tid [27]
static TOID(struct version_list) find_lts(int tid, TOID(struct rb_node) node) {
  TOID(struct version_list) closest_tuple = TOID_NULL(struct version_list);
  TOID(struct version_list)
  current_vl = D_RO(node)->vl; // Start from node's version list [27]

  while (!TOID_IS_NULL(current_vl)) {
    if (D_RO(current_vl)->ts < tid) { // [27]
      // If current_vl is newer than closest_tuple found so far, update closest
      if (TOID_IS_NULL(closest_tuple) ||
          D_RO(current_vl)->ts > D_RO(closest_tuple)->ts) {
        closest_tuple = current_vl; // [27]
      }
    }
    current_vl = D_RO(current_vl)->vnext; // Move to next version
  }
  return closest_tuple;
}

// Checks if any higher timestamp transaction already read this version [24]
static bool check_versions(int tid, TOID(struct rb_node) node) {
  TOID(struct version_list) closest_tuple = find_lts(tid, node); // [24]

  if (TOID_IS_NULL(closest_tuple)) {
    // If no prior version is found with ts < tid, there's no conflict based on
    // rvl. This is important for the "0th version" concept for opacity in
    // lookups.
    return true;
  }

  for (int i = 0; i < 16; ++i) { // Iterate over rvl (fixed size 16)
    // Check for any higher timestamp in rvl [40]
    if (D_RO(closest_tuple)->rvl[i] != 0 && D_RO(closest_tuple)->rvl[i] > tid) {
      return false; // Higher timestamp found, conflict [40]
    }
  }
  return true; // No conflict found [40]
}

// Inserts a node into the lazyrb-list [14]
static TOID(struct rb_node)
    list_ins(TOID(struct rb_node) preds_RL, TOID(struct rb_node) currs_RL,
             TOID(struct rb_node) preds_BL, TOID(struct rb_node) currs_BL,
             TOID(struct rb_node) new_node, list_insert_type_t list_type) {
  if (list_type == LIST_TYPE_RL_ONLY) { // [14] (RL only)
    TX_SET(new_node, RL, currs_RL);
    TX_SET(preds_RL, RL, new_node);
  } else if (list_type == LIST_TYPE_RL_BL) { // [14] (Red as well as Blue List)
    TX_SET(new_node, RL, currs_RL);
    TX_SET(new_node, BL, currs_BL);
    TX_SET(preds_RL, RL, new_node);
    TX_SET(preds_BL, BL, new_node);
  } else if (list_type ==
             LIST_TYPE_RL_BL_FROM_RL_TO_BL) { // Relink existing RL node into BL
                                              // [14] (Algo 17, lines 323-325)
    // This means new_node is already connected via RL. We are now adding it to
    // BL. It implies `new_node` here corresponds to `currs` in Algorithm 17.
    // `preds_BL` corresponds to `preds`, `currs_BL` corresponds to `currs[8]`.
    // `preds_RL` and `currs_RL` are typically the same `preds` and `currs` from
    // the RL traversal.
    TX_SET(preds_BL, BL, new_node);
    TX_SET(new_node, BL, currs_BL);
  }
  // No explicit lock() calls here as they are handled by
  // acquire_pred_curr_locks before this function is called.
  return new_node;
}

// Deletes a node from blue link by bypassing it [18]
static void list_del_bl(TOID(struct rb_node) preds_BL,
                        TOID(struct rb_node) currs_BL) {
  // Updates the blue links to bypass currs_BL [18]
  TX_SET(preds_BL, BL, currs_BL);
}

// Common logic for STM lookup and STM delete when key is not in local log [11]
static void common_lu_del(int obj_id, int key, int *val, status_t *op_status) {
  TOID(struct rb_node) preds[6];
  TOID(struct rb_node) currs[6];
  bool retry;

  do {
    retry = false;
    list_lookup_internal(key, preds, currs); // [11, 39]
    acquire_pred_curr_locks(preds, currs);   // [11, 20]

    if (!method_validation(preds, currs)) {  // [11, 21]
      release_pred_curr_locks(preds, currs); // [11, 14]
      retry = true;
    }
  } while (retry);

  // After successful validation and lock acquisition:
  if (D_RO(currs[8])->key == key) { // Node found in BL (unmarked/live) [11]
    TOID(struct rb_node) node = currs[8];
    TOID(struct version_list)
    closest_tuple = find_lts(local_txlog->tid, node); // [11, 27]

    // If no version found or it's a marked version, set status FAIL
    if (TOID_IS_NULL(closest_tuple) ||
        D_RO(closest_tuple)->mark == true) { // [41]
      *val = 0;
      *op_status = FAIL; // [41]
    } else {
      *val = D_RO(closest_tuple)->val; // [41]
      *op_status = OK;                 // [41]
    }
    // As per Algo 1 Line 22, add current transaction ID to rvl of
    // closest_tuple. This is a modification that needs to be transactional. For
    // lookup, this happens only on the persistent version if found. For
    // simplicity with PMDK, `TX_ADD_FIELD` is conceptual. A real `rvl` needs
    // dynamic management.
    if (!TOID_IS_NULL(closest_tuple)) {
      TX_BEGIN(pop) {
        // Find a free slot in rvl or replace an old one
        bool rvl_added = false;
        for (int i = 0; i < 16; ++i) {
          if (D_RO(closest_tuple)->rvl[i] == 0) { // Assuming 0 means empty slot
            TX_SET_FIELD(closest_tuple, rvl[i], local_txlog->tid);
            rvl_added = true;
            break;
          }
        }
        if (!rvl_added) { // If rvl is full, replace oldest or handle
                          // (simplified)
          TX_SET_FIELD(closest_tuple, rvl,
                       local_txlog->tid); // Simple overwrite if full
        }
      }
      TX_ONABORT {
        fprintf(stderr, "common_lu_del: Failed to update rvl for key %d\n",
                key);
      }
      TX_END
    }

  } else if (D_RO(currs)->key ==
             key) { // Node found only in RL (marked or 0-version) [41]
    TOID(struct rb_node) node = currs;
    TOID(struct version_list)
    closest_tuple = find_lts(local_txlog->tid, node); // [27, 41]

    // Marked node usually returns FAIL for lookup [29]
    if (TOID_IS_NULL(closest_tuple) || D_RO(closest_tuple)->mark == true) {
      *val = 0;
      *op_status = FAIL; // [29]
    } else {             // Reading an unmarked version of a RL-only node
      *val = D_RO(closest_tuple)->val;
      *op_status = OK;
    }
    // Similar rvl update as above.
    if (!TOID_IS_NULL(closest_tuple)) {
      TX_BEGIN(pop) {
        bool rvl_added = false;
        for (int i = 0; i < 16; ++i) {
          if (D_RO(closest_tuple)->rvl[i] == 0) {
            TX_SET_FIELD(closest_tuple, rvl[i], local_txlog->tid);
            rvl_added = true;
            break;
          }
        }
        if (!rvl_added) {
          TX_SET_FIELD(closest_tuple, rvl, local_txlog->tid);
        }
      }
      TX_ONABORT {
        fprintf(stderr, "common_lu_del: Failed to update rvl for key %d\n",
                key);
      }
      TX_END
    }
  } else { // Key not in lazyrb-list (needs 0th version creation) [29]
    TOID(struct rb_node) new_node;
    TOID(struct version_list) zero_vl;
    TOID(struct rb_root) root_oid = POBJ_ROOT(pop, struct rb_root);

    TX_BEGIN(pop) {                       // This part must be transactional
      new_node = TX_ZNEW(struct rb_node); // [29]
      TX_SET(new_node, key, key);
      TX_SET(new_node, marked, true); // Marked for lazy deletion [28, 29]
      pthread_mutex_init(&D_RW(new_node)->lock,
                         NULL); // Initialize mutex for new node

      zero_vl = TX_ZNEW(struct version_list); // [29]
      TX_SET(zero_vl, ts, 0);  // Timestamp 0 for this special version [28, 29]
      TX_SET(zero_vl, val, 0); // Null value [28, 29]
      TX_SET(zero_vl, mark, true); // Mark this version as a deletion [28, 29]
      memset(D_RW(zero_vl)->rvl, 0, sizeof(D_RW(zero_vl)->rvl)); // Clear rvl
      TX_SET_FIELD(zero_vl, rvl,
                   local_txlog->tid); // Add current transaction's ID [42]
      TX_SET(zero_vl, vnext, TOID_NULL(struct version_list));

      TX_SET(new_node, vl, zero_vl); // Link version list to node

      // Insert into RL only (as per commonLu&Del, Algo 11, line 198) [42]
      // This implicitly calls list_ins with LIST_TYPE_RL_ONLY
      TX_SET(preds[8], RL, new_node);
      TX_SET(new_node, RL, currs);
      TX_SET(new_node, BL,
             TOID_NULL(struct rb_node)); // Explicitly no BL for 0th version
      // Head and Tail keys cannot be changed.
      TX_ADD(root_oid, node_count, 1);

      *val = 0;
      *op_status = FAIL; // [42]
    }
    TX_ONABORT {
      fprintf(stderr, "common_lu_del: Failed to create 0th version node\n");
      *val = 0;
      *op_status = ABORT; // Indicate failure
    }
    TX_END

    // Store the newly created node in the local record (for future operations
    // in same tx)
    struct local_rec *rec = find_record(obj_id, key);
    if (rec) {
      rec->node = new_node;
    }
  }
  release_pred_curr_locks(preds, currs); // [14, 42]
}

// Performs intra-transaction validation and updates preds/currs for subsequent
// operations [25]
static void intra_trans_validation(struct local_rec *rec_current,
                                   TOID(struct rb_node) preds_current[],
                                   TOID(struct rb_node) currs_current[]) {
  // This function aims to update the `preds_current` and `currs_current`
  // of `rec_current` if previous operations in the *same transaction* have
  // altered the path to `rec_current->key`.
  // The source (Algorithm 23 [25]) is abstract on how `L reck` (previous
  // record) is accessed. This simplified implementation re-looks up the path if
  // validation fails, and updates the local record.

  // Validate path correctness first.
  if (D_RO(preds_current)->marked ||
      !TOID_EQUALS(D_RO(preds_current)->BL, currs_current[8]) ||
      !TOID_EQUALS(D_RO(preds_current[8])->RL, currs_current)) {

    // If the path is no longer valid, re-find it.
    TOID(struct rb_node) updated_preds[6];
    TOID(struct rb_node) updated_currs[6];
    list_lookup_internal(rec_current->key, updated_preds, updated_currs);

    // Update the current record's preds/currs in the local log and the passed
    // arrays.
    set_preds_currs(rec_current->obj_id, rec_current->key, updated_preds,
                    updated_currs);
    preds_current = updated_preds;
    preds_current[8] = updated_preds[8];
    currs_current = updated_currs;
    currs[8] = updated_currs[8];
  }
}

// Release all locks in increasing order of their keys for records in the local
// log [35]
static void release_ordered_locks(struct tx_log *log) {
  TOID(struct rb_node) nodes_to_release[log->record_count * 4]; // Max possible
  size_t count = 0;

  // Collect all distinct nodes (preds and currs) from all records in the log
  for (size_t i = 0; i < log->record_count; ++i) {
    struct local_rec *rec = &log->records[i];
    if (!TOID_IS_NULL(rec->preds))
      nodes_to_release[count++] = rec->preds;
    if (!TOID_IS_NULL(rec->preds[8]))
      nodes_to_release[count++] = rec->preds[8];
    if (!TOID_IS_NULL(rec->currs))
      nodes_to_release[count++] = rec->currs;
    if (!TOID_IS_NULL(rec->currs[8]))
      nodes_to_release[count++] = rec->currs[8];
  }

  // Sort distinct nodes by key to release in acquired order
  for (size_t i = 0; i < count; ++i) {
    for (size_t j = i + 1; j < count; ++j) {
      if (D_RO(nodes_to_release[i])->key > D_RO(nodes_to_release[j])->key) {
        TOID(struct rb_node) temp = nodes_to_release[i];
        nodes_to_release[i] = nodes_to_release[j];
        nodes_to_release[j] = temp;
      }
    }
  }

  // Release locks on distinct sorted nodes
  if (count > 0) {
    pthread_mutex_unlock(&D_RW(nodes_to_release)->lock); // [35]
    for (size_t i = 1; i < count; ++i) {
      if (!TOID_EQUALS(nodes_to_release[i], nodes_to_release[i - 1])) {
        pthread_mutex_unlock(&D_RW(nodes_to_release[i])->lock); // [35]
      }
    }
  }
}

// tryC Validation (called by upd method in tryC) [23]
static bool tryc_validation(int tid, TOID(struct rb_node) preds[],
                            TOID(struct rb_node) currs[]) {
  if (!method_validation(preds, currs)) { // [21, 43]
    return false;
  }

  // If key exists and there's a higher timestamp in rvl of closest tuple [24,
  // 43]
  TOID(struct rb_node) node_to_check = TOID_NULL(struct rb_node);
  if (D_RO(currs[8])->key == local_txlog->records.key) { // Node in BL
    node_to_check = currs[8];
  } else if (D_RO(currs)->key == local_txlog->records.key) { // Node in RL
    node_to_check = currs;
  }

  if (!TOID_IS_NULL(node_to_check) &&
      (D_RO(node_to_check)->key == local_txlog->records.key)) {
    if (!check_versions(tid, node_to_check)) { // [24, 43]
      return false; // Higher timestamp found in rvl, abort.
    }
  }
  return true; // [43]
}

// --- Main PTM Operations ---

// Initialize the ptm system [44]
int ptm_init(void) {
  const char *pool_path = "/tmp/rb_list_pool";
  const size_t pool_size = 256 * 1024 * 1024; // 256MB [44]

  pop = pmemobj_create(pool_path, POBJ_LAYOUT_NAME(rb_list_layout), pool_size,
                       0666); // [44]
  if (pop == NULL) {
    pop = pmemobj_open(pool_path, POBJ_LAYOUT_NAME(rb_list_layout)); // [44]
    if (pop == NULL) {
      perror("Failed to create/open pool");
      return -1;
    }
  }

  TOID(struct rb_root) root_oid = POBJ_ROOT(pop, struct rb_root);

  // Initialize head and tail sentinel nodes if root is null or not initialized
  if (TOID_IS_NULL(D_RO(root_oid)->head)) {
    TX_BEGIN(pop) {
      TOID(struct rb_node) head_node = TX_ZNEW(struct rb_node);
      TOID(struct rb_node) tail_node = TX_ZNEW(struct rb_node);

      TX_SET(head_node, key, NEG_INF);
      TX_SET(head_node, marked, false);
      pthread_mutex_init(&D_RW(head_node)->lock,
                         NULL);         // Initialize mutex for head
      TX_SET(head_node, RL, tail_node); // Head's RL points to tail [5]
      TX_SET(head_node, BL, tail_node); // Head's BL points to tail [5]
      TX_SET(head_node, vl, TOID_NULL(struct version_list));

      TX_SET(tail_node, key, POS_INF);
      TX_SET(tail_node, marked, false);
      pthread_mutex_init(&D_RW(tail_node)->lock,
                         NULL); // Initialize mutex for tail
      TX_SET(tail_node, RL,
             TOID_NULL(struct rb_node)); // Tail's RL points to NULL
      TX_SET(tail_node, BL,
             TOID_NULL(struct rb_node)); // Tail's BL points to NULL
      TX_SET(tail_node, vl, TOID_NULL(struct version_list));

      TX_SET(root_oid, head, head_node); // Set root's head
      TX_SET(root_oid, tail, tail_node); // Set root's tail

      atomic_init(&D_RW(root_oid)->global_counter, 1); // [44]
      TX_SET(root_oid, node_count, 0);                 // [44]
    }
    TX_END
  }
  return 0;
}

// Begin a new transaction [45]
int ptm_begin(void) {
  if (local_txlog == NULL) {                     // [45]
    local_txlog = malloc(sizeof(struct tx_log)); // [45]
    if (local_txlog == NULL) {
      perror("Failed to allocate local_txlog");
      return -1;
    }
    local_txlog->records = malloc(sizeof(struct local_rec) * 16); // [45]
    if (local_txlog->records == NULL) {
      perror("Failed to allocate local_txlog records");
      free(local_txlog);
      local_txlog = NULL;
      return -1;
    }
    local_txlog->record_capacity = 16; // [45]
    local_txlog->record_count = 0;     // [45]
  }
  TOID(struct rb_root) root = POBJ_ROOT(pop, struct rb_root);
  local_txlog->tid = atomic_fetch_add(&D_RW(root)->global_counter, 1); // [45]
  local_txlog->tx_status = OK;                                         // [45]
  local_txlog->record_count = 0;                                       // [45]
  return local_txlog->tid;                                             // [45]
}

// ptm Insert operation [46]
int ptm_insert(int obj_id, int key, int val) {
  struct local_rec *rec = find_record(obj_id, key); // [46]
  if (rec == NULL) {                                // [46]
    if (create_record(obj_id, key) != 0) {          // [46]
      return -1;
    }
    rec = find_record(obj_id, key); // Re-find after creation
  }
  set_val(obj_id, key, val);      // [46]
  set_opn(obj_id, key, INSERT);   // [46]
  set_op_status(obj_id, key, OK); // [46]
  return 0;
}

// ptm Delete operation [46]
int ptm_delete(int obj_id, int key) {
  struct local_rec *rec = find_record(obj_id, key); // [46]
  if (rec == NULL) {                                // [46]
    // Need to search in the persistent structure to determine if key exists
    // [46]
    TOID(struct rb_node) preds[6], currs[6];
    list_lookup_internal(key, preds, currs); // Find key's location

    // A key exists if it's either the current node in BL or RL
    if (D_RO(currs[8])->key != key && D_RO(currs)->key != key) {
      return -1; // Key not found
    }

    if (create_record(obj_id, key) != 0) { // [47]
      return -1;
    }
    rec = find_record(obj_id, key); // Re-find after creation
    set_preds_currs(obj_id, key, preds,
                    currs); // Store search results for commit
  }
  set_opn(obj_id, key, DELETE);   // [47]
  set_op_status(obj_id, key, OK); // [47]
  return 0;
}

// ptm Lookup operation [47]
int ptm_lookup(int obj_id, int key, int *val, status_t *op_status) {
  struct local_rec *rec = find_record(obj_id, key); // [48]
  if (rec != NULL) {                                // [48]
    // Found in local log
    op_name_t prev_op = get_opn(obj_id, key);     // [48]
    if (prev_op == INSERT || prev_op == LOOKUP) { // [48]
      *val = get_val(obj_id, key);                // [48]
      *op_status = get_op_status(obj_id, key);    // [48]
    } else if (prev_op == DELETE) {               // [48]
      *val = 0;                                   // [48]
      *op_status = FAIL;                          // [48]
    }
  } else { // Search in persistent structure [48]
    // Calls common_lu_del to handle persistent lookup and 0th version insertion
    // [11]
    common_lu_del(obj_id, key, val, op_status);

    // Create record for this lookup [49]
    if (create_record(obj_id, key) == 0) { // [49]
      set_val(obj_id, key, *val);          // [49]
      // Re-get preds/currs after `common_lu_del` to ensure local log has latest
      // path info
      TOID(struct rb_node) preds[6], currs[6];
      list_lookup_internal(key, preds, currs);
      set_preds_currs(obj_id, key, preds,
                      currs); // Store search results for commit
    }
  }
  set_opn(obj_id, key, LOOKUP);           // [49]
  set_op_status(obj_id, key, *op_status); // [49]
  return 0;
}

// Actual insert operation (called during commit) [15]
static int perform_insert(int key, int val) {
  TOID(struct rb_root) root_oid = POBJ_ROOT(pop, struct rb_root);
  TOID(struct rb_node) new_node;
  TOID(struct version_list) vl;

  // Get the preds and currs for this key from the local log (set during
  // ptm_insert)
  TOID(struct rb_node) preds[6];
  TOID(struct rb_node) currs[6];
  get_preds_currs(0, key, preds,
                  currs); // obj_id is arbitrary here, assuming it's consistent

  // Logic from Algo 12 (STM tryC), lines 240-250 for INSERT [12, 13]
  if (D_RO(currs[8])->key == key) { // Node already in BL (unmarked/live) [12]
    // Add new version to existing node
    TOID(struct rb_node) existing_node = currs[8];
    vl = TX_ZNEW(struct version_list);
    TX_SET(vl, ts, local_txlog->tid);
    TX_SET(vl, val, val);
    TX_SET(vl, mark, false);
    TX_SET(vl, vnext, D_RO(existing_node)->vl); // Prepend new version
    TX_SET(existing_node, vl, vl);
    TX_SET(existing_node, marked, false); // Ensure it's unmarked
  } else if (D_RO(currs)->key ==
             key) { // Node found only in RL (marked or 0-version) [12]
    // This node needs to be re-inserted into the blue list, and a new version
    // added.
    TOID(struct rb_node) existing_node = currs; // This is the marked node
    // Re-link into BL (make it accessible via BL)
    list_ins(preds[8], currs, preds, currs[8], existing_node,
             LIST_TYPE_RL_BL_FROM_RL_TO_BL);
    vl = TX_ZNEW(struct version_list);
    TX_SET(vl, ts, local_txlog->tid);
    TX_SET(vl, val, val);
    TX_SET(vl, mark, false);
    TX_SET(vl, vnext, D_RO(existing_node)->vl);
    TX_SET(existing_node, vl, vl);
    TX_SET(existing_node, marked, false); // Ensure it's unmarked
  } else { // Key not found, create new node and insert into BL and RL [13]
    new_node = TX_ZNEW(struct rb_node); // [15]
    TX_SET(new_node, key, key);
    TX_SET(new_node, marked, false);
    pthread_mutex_init(&D_RW(new_node)->lock,
                       NULL); // Initialize mutex for new node

    vl = TX_ZNEW(struct version_list); // [50]
    TX_SET(vl, ts, local_txlog->tid);
    TX_SET(vl, val, val);
    TX_SET(vl, mark, false);
    TX_SET(vl, vnext, TOID_NULL(struct version_list)); // First version
    TX_SET(new_node, vl, vl);

    list_ins(preds[8], currs, preds, currs[8], new_node,
             LIST_TYPE_RL_BL);       // Insert into both lists
    TX_ADD(root_oid, node_count, 1); // [51]
  }
  return 0;
}

// Actual delete operation (called during commit) - lazy deletion [51]
static int perform_delete(int key) {
  TOID(struct rb_node) node_to_delete = TOID_NULL(struct rb_node);
  TOID(struct rb_node) preds[6];
  TOID(struct rb_node) currs[6];

  // Get the preds and currs for this key from the local log (set during
  // ptm_delete)
  get_preds_currs(0, key, preds,
                  currs); // obj_id is arbitrary here, assuming it's consistent

  // A delete operation can only occur if the node is in the list
  if (D_RO(currs[8])->key == key) { // Node is in BL (unmarked)
    node_to_delete = currs[8];
  } else if (D_RO(currs)->key ==
             key) { // Node is only in RL (already marked or 0-version)
    node_to_delete = currs;
  }

  if (TOID_IS_NULL(node_to_delete)) {
    return -1; // Key not found for deletion
  }

  // Mark as deleted lazily [16]
  TX_SET(node_to_delete, marked, true);

  // Add deletion version to version list [16]
  TOID(struct version_list) vl = TX_ZNEW(struct version_list); // [16]
  TX_SET(vl, ts, local_txlog->tid);
  TX_SET(vl, val, 0);     // Null value for deletion [16]
  TX_SET(vl, mark, true); // Mark this version as a deletion [16]
  TX_SET(vl, vnext,
         D_RO(node_to_delete)->vl); // Prepend new deletion version [16]
  TX_SET(node_to_delete, vl, vl);

  // Remove from blue list by updating predecessor's BL to bypass [18]
  // This is `lslDel(preds[] ↓, currs[] ↓)` [18]
  // Only applies if the node was actually in the BL (currs[8])
  if (TOID_EQUALS(node_to_delete, currs[8])) {
    list_del_bl(preds, currs[8]);
  }

  return 0;
}

// Try to commit the transaction [49]
int ptm_try_commit(void) {
  if (local_txlog == NULL || local_txlog->record_count == 0) { // [49]
    return 0; // Nothing to commit
  }

  // Acquire locks on all relevant nodes (preds/currs) in order [52, 53]
  for (size_t i = 0; i < local_txlog->record_count; i++) {
    struct local_rec *rec = &local_txlog->records[i];
    TOID(struct rb_node) current_preds[6];
    TOID(struct rb_node) current_currs[6];

    // For each record, re-find its location and acquire locks
    list_lookup_internal(rec->key, current_preds, current_currs); // [53]
    acquire_pred_curr_locks(current_preds, current_currs);        // [22]

    // Update the record's stored preds/currs with the freshly acquired ones
    set_preds_currs(rec->obj_id, rec->key, current_preds, current_currs);

    // Validation for each record's path [22]
    if (!tryc_validation(local_txlog->tid, current_preds,
                         current_currs)) { // [22, 23]
      release_ordered_locks(
          local_txlog); // Release all acquired locks if validation fails [54]
      local_txlog->tx_status = ABORT; // [54]
      return -1;                      // Abort
    }
  }

  // Perform actual operations within a persistent transaction [52]
  TX_BEGIN(pop) {                                            // [52]
    for (size_t i = 0; i < local_txlog->record_count; i++) { // [52]
      struct local_rec *rec = &local_txlog->records[i];

      // Perform intra-transaction validation before executing the operation
      // [55] Pass the current record itself to intra_trans_validation
      TOID(struct rb_node) rec_preds[6];
      TOID(struct rb_node) rec_currs[6];
      get_preds_currs(rec->obj_id, rec->key, rec_preds,
                      rec_currs); // Get stored preds/currs from log
      intra_trans_validation(rec, rec_preds,
                             rec_currs); // Adjust preds/currs if needed [25]

      switch (rec->opn) { // [52]
      case INSERT:
        perform_insert(rec->key, rec->val); // [52]
        break;
      case DELETE:
        perform_delete(rec->key); // [52]
        break;
      case LOOKUP:
        // Lookup doesn't modify, just validation. Validation already done. [52]
        break;
      }
    }
  }
  TX_ONABORT { // [52]
    // Release locks and abort [54]
    release_ordered_locks(local_txlog); // [54]
    local_txlog->tx_status = ABORT;     // [54]
    return -1;                          // [54]
  }
  TX_END

  // Release locks [54]
  release_ordered_locks(local_txlog); // [54]
  local_txlog->tx_status = COMMIT;    // [54]
  local_txlog->record_count = 0;      // Clear transaction log [54]
  return 0;
}

// Abort the transaction [30]
void ptm_abort(void) {
  if (local_txlog != NULL) {        // [30]
    local_txlog->tx_status = ABORT; // [30]
    local_txlog->record_count = 0;  // [30]
  }
}

// Cleanup function [56]
void ptm_cleanup(void) {
  if (local_txlog) {            // [56]
    free(local_txlog->records); // [56]
    free(local_txlog);          // [56]
    local_txlog = NULL;         // [56]
  }
  if (pop) {            // [56]
    pmemobj_close(pop); // [56]
    pop = NULL;         // [56]
  }
}

// Example usage
int main() {
  if (ptm_init() != 0) { // [56]
    fprintf(stderr, "Failed to initialize ptm\n");
    return 1;
  }

  // Begin transaction
  int tid = ptm_begin();                   // [56]
  printf("Started transaction %d\n", tid); // [56]

  // Insert some values
  ptm_insert(1, 10, 100); // [56]
  ptm_insert(1, 20, 200); // [56]
  ptm_insert(1, 30, 300); // [56]

  // Lookup
  int val;
  status_t status;
  ptm_lookup(1, 20, &val, &status);                          // [56]
  printf("Lookup key 20: val=%d, status=%d\n", val, status); // [56]

  // Delete
  ptm_delete(1, 20); // [57]

  // Try to commit
  if (ptm_try_commit() == 0) {                      // [57]
    printf("Transaction committed successfully\n"); // [57]
  } else {
    printf("Transaction aborted\n"); // [57]
  }

  // Start a new transaction for verification after commit
  ptm_begin();
  ptm_lookup(1, 10, &val, &status);
  printf("Lookup key 10 after commit: val=%d, status=%d\n", val, status);
  ptm_lookup(1, 20, &val, &status); // Should be FAIL now
  printf("Lookup key 20 after commit: val=%d, status=%d\n", val, status);
  ptm_lookup(1, 30, &val, &status);
  printf("Lookup key 30 after commit: val=%d, status=%d\n", val, status);
  ptm_try_commit(); // Commit the lookup transaction

  ptm_cleanup(); // [57]
  return 0;
}