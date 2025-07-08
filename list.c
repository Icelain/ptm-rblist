#include <libpmemobj.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// Pool layout declaration
POBJ_LAYOUT_BEGIN(skip_list_layout);
POBJ_LAYOUT_TOID(skip_list_layout, struct skip_node);
POBJ_LAYOUT_TOID(skip_list_layout, struct version_list);
POBJ_LAYOUT_ROOT(skip_list_layout, struct skip_root);
POBJ_LAYOUT_END(skip_list_layout);

// Skip list constants
#define MAX_LEVEL 16
#define SKIP_LIST_P 0.5

// Operation types from MVOSTM
typedef enum { INSERT = 0, DELETE = 1, LOOKUP = 2 } op_name_t;

// Status types from MVOSTM
typedef enum { ABORT = 0, OK = 1, FAIL = 2, COMMIT = 3 } status_t;

// Global persistent node structure (based on MVOSTM G_node)
struct skip_node {
  int key;                                   // G_key
  TOID(struct version_list) vl;              // G_vl - version list
  pthread_mutex_t lock;                      // G_lock
  int level;                                 // Skip list level
  TOID(struct skip_node) forward[MAX_LEVEL]; // Forward pointers array
  bool marked;                               // Lazy deletion mark
};

// Version list structure (based on MVOSTM G_vl)
struct version_list {
  int ts;                          // G_ts - timestamp
  int val;                         // G_val - value
  bool mark;                       // G_mark - deletion mark
  int rvl[16];                     // G_rvl - return value list
  TOID(struct version_list) vnext; // G_vnext - next version
};

// Root structure for the persistent skip list
struct skip_root {
  TOID(struct skip_node) header; // Header node
  int current_level;             // Current max level
  atomic_int global_counter;     // G_cnt from MVOSTM
  size_t node_count;
};

// Local transaction record (based on MVOSTM L_rec)
struct local_rec {
  int obj_id;                              // L_obj_id
  int key;                                 // L_key
  int val;                                 // L_val
  TOID(struct skip_node) pred;             // L_pred
  TOID(struct skip_node) curr;             // L_curr
  TOID(struct skip_node) node;             // node
  status_t op_status;                      // L_op_status
  op_name_t opn;                           // L_opn
  TOID(struct skip_node) preds[MAX_LEVEL]; // Predecessor nodes at each level
  TOID(struct skip_node) succs[MAX_LEVEL]; // Successor nodes at each level
};

// Local transaction log (based on MVOSTM L_txlog)
struct tx_log {
  int tid;                   // L_tid
  status_t tx_status;        // L_tx_status
  struct local_rec *records; // L_list
  size_t record_count;
  size_t record_capacity;
};

// Global variables
static PMEMobjpool *pop = NULL;
static __thread struct tx_log *local_txlog = NULL;

// Function prototypes
int stm_init(void);
int stm_begin(void);
int stm_insert(int obj_id, int key, int val);
int stm_delete(int obj_id, int key);
int stm_lookup(int obj_id, int key, int *val, status_t *op_status);
int stm_try_commit(void);
void stm_abort(void);

// Helper functions
static struct local_rec *find_record(int obj_id, int key);
static int create_record(int obj_id, int key);
static void set_opn(int obj_id, int key, op_name_t op);
static void set_val(int obj_id, int key, int val);
static void set_op_status(int obj_id, int key, status_t status);
static void set_pred_curr(int obj_id, int key, TOID(struct skip_node) pred,
                          TOID(struct skip_node) curr);
static op_name_t get_opn(int obj_id, int key);
static int get_val(int obj_id, int key);
static status_t get_op_status(int obj_id, int key);
static void get_pred_curr(int obj_id, int key, TOID(struct skip_node) * pred,
                          TOID(struct skip_node) * curr);

// Skip list operations
static int random_level(void);
static bool skip_search(int key, TOID(struct skip_node) preds[MAX_LEVEL],
                        TOID(struct skip_node) succs[MAX_LEVEL]);
static TOID(struct skip_node) skip_find(int key);
static int perform_insert(int key, int val);
static int perform_delete(int key);
static void common_lu_del(int obj_id, int key, int *val, status_t *op_status);

// Initialize the STM system
int stm_init(void) {
  const char *pool_path = "/tmp/skip_list_pool";
  const size_t pool_size = 256 * 1024 * 1024; // 256MB

  // Initialize random seed
  srand(time(NULL));

  pop = pmemobj_create(pool_path, POBJ_LAYOUT_NAME(skip_list_layout), pool_size,
                       0666);
  if (pop == NULL) {
    pop = pmemobj_open(pool_path, POBJ_LAYOUT_NAME(skip_list_layout));
    if (pop == NULL) {
      perror("Failed to create/open pool");
      return -1;
    }
  }

  TOID(struct skip_root) root = POBJ_ROOT(pop, struct skip_root);

  // Initialize root if first time
  if (TOID_IS_NULL(D_RO(root)->header)) {
    TX_BEGIN(pop) {
      // Create header node
      TOID(struct skip_node) header = TX_ZNEW(struct skip_node);
      TX_SET(header, key, INT_MIN);
      TX_SET(header, level, MAX_LEVEL);
      TX_SET(header, marked, false);

      // Initialize forward pointers to NULL
      for (int i = 0; i < MAX_LEVEL; i++) {
        TX_SET_DIRECT(&D_RW(header)->forward[i], TOID_NULL(struct skip_node));
      }

      // Initialize mutex
      pthread_mutex_init(&D_RW(header)->lock, NULL);

      // Set root fields
      TX_SET(root, header, header);
      TX_SET(root, current_level, 1);
      atomic_init(&D_RW(root)->global_counter, 1);
      TX_SET(root, node_count, 0);
    }
    TX_END
  }

  return 0;
}

// Begin a new transaction
int stm_begin(void) {
  if (local_txlog == NULL) {
    local_txlog = malloc(sizeof(struct tx_log));
    local_txlog->records = malloc(sizeof(struct local_rec) * 16);
    local_txlog->record_capacity = 16;
    local_txlog->record_count = 0;
  }

  TOID(struct skip_root) root = POBJ_ROOT(pop, struct skip_root);
  local_txlog->tid = atomic_fetch_add(&D_RW(root)->global_counter, 1);
  local_txlog->tx_status = OK;
  local_txlog->record_count = 0;

  return local_txlog->tid;
}

// STM Insert operation (based on MVOSTM Algorithm 8)
int stm_insert(int obj_id, int key, int val) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec == NULL) {
    if (create_record(obj_id, key) != 0) {
      return -1;
    }
    rec = find_record(obj_id, key);
  }

  // Linearization point - set value in local log
  set_val(obj_id, key, val);
  set_opn(obj_id, key, INSERT);
  set_op_status(obj_id, key, OK);

  return 0;
}

// STM Delete operation
int stm_delete(int obj_id, int key) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec == NULL) {
    if (create_record(obj_id, key) != 0) {
      return -1;
    }
    rec = find_record(obj_id, key);
  }

  set_opn(obj_id, key, DELETE);
  set_op_status(obj_id, key, OK);

  return 0;
}

// STM Lookup operation (based on MVOSTM Algorithm 9)
int stm_lookup(int obj_id, int key, int *val, status_t *op_status) {
  struct local_rec *rec = find_record(obj_id, key);

  if (rec != NULL) {
    // Found in local log - check previous operation
    op_name_t prev_op = get_opn(obj_id, key);

    if (prev_op == INSERT || prev_op == LOOKUP) {
      // If previous operation was insert/lookup, return its value/status
      *val = get_val(obj_id, key);
      *op_status = get_op_status(obj_id, key);
    } else if (prev_op == DELETE) {
      // If previous operation was delete, return NULL/FAIL
      *val = 0;
      *op_status = FAIL;
    }
  } else {
    // Not in local log - call common lookup/delete function
    common_lu_del(obj_id, key, val, op_status);
  }

  // Update local log with lookup operation
  set_opn(obj_id, key, LOOKUP);
  set_op_status(obj_id, key, *op_status);

  return 0;
}

// Common lookup and delete function (similar to MVOSTM commonLu&Del)
static void common_lu_del(int obj_id, int key, int *val, status_t *op_status) {
  TOID(struct skip_node) preds[MAX_LEVEL];
  TOID(struct skip_node) succs[MAX_LEVEL];

  // Search in skip list
  bool found = skip_search(key, preds, succs);

  if (found && !D_RO(succs[0])->marked) {
    // Found and not marked for deletion
    TOID(struct version_list) vl = D_RO(succs[0])->vl;
    if (!TOID_IS_NULL(vl)) {
      *val = D_RO(vl)->val;
      *op_status = OK;
    } else {
      *val = 0;
      *op_status = FAIL;
    }
  } else {
    // Not found or marked for deletion
    *val = 0;
    *op_status = FAIL;
  }

  // Create record if not exists
  if (find_record(obj_id, key) == NULL) {
    if (create_record(obj_id, key) == 0) {
      set_val(obj_id, key, *val);
      // Store predecessors and successors for commit phase
      struct local_rec *rec = find_record(obj_id, key);
      if (rec) {
        for (int i = 0; i < MAX_LEVEL; i++) {
          rec->preds[i] = preds[i];
          rec->succs[i] = succs[i];
        }
      }
    }
  }
}

// Try to commit the transaction
int stm_try_commit(void) {
  if (local_txlog == NULL || local_txlog->record_count == 0) {
    return 0;
  }

  // Sort records by key for ordered locking
  // Simple bubble sort for demonstration
  for (size_t i = 0; i < local_txlog->record_count - 1; i++) {
    for (size_t j = 0; j < local_txlog->record_count - i - 1; j++) {
      if (local_txlog->records[j].key > local_txlog->records[j + 1].key) {
        struct local_rec temp = local_txlog->records[j];
        local_txlog->records[j] = local_txlog->records[j + 1];
        local_txlog->records[j + 1] = temp;
      }
    }
  }

  // Acquire locks on all relevant nodes in order
  for (size_t i = 0; i < local_txlog->record_count; i++) {
    struct local_rec *rec = &local_txlog->records[i];

    // Lock predecessors and successors
    for (int level = 0; level < MAX_LEVEL; level++) {
      if (!TOID_IS_NULL(rec->preds[level])) {
        pthread_mutex_lock(&D_RW(rec->preds[level])->lock);
      }
      if (!TOID_IS_NULL(rec->succs[level])) {
        pthread_mutex_lock(&D_RW(rec->succs[level])->lock);
      }
    }
  }

  // Validate and perform actual operations
  TX_BEGIN(pop) {
    for (size_t i = 0; i < local_txlog->record_count; i++) {
      struct local_rec *rec = &local_txlog->records[i];

      switch (rec->opn) {
      case INSERT:
        if (perform_insert(rec->key, rec->val) != 0) {
          pmemobj_tx_abort(EINVAL);
        }
        break;
      case DELETE:
        if (perform_delete(rec->key) != 0) {
          pmemobj_tx_abort(EINVAL);
        }
        break;
      case LOOKUP:
        // Lookup doesn't modify, just validation
        break;
      }
    }
  }
  TX_ONABORT {
    // Release locks and abort
    for (size_t i = 0; i < local_txlog->record_count; i++) {
      struct local_rec *rec = &local_txlog->records[i];
      for (int level = 0; level < MAX_LEVEL; level++) {
        if (!TOID_IS_NULL(rec->preds[level])) {
          pthread_mutex_unlock(&D_RW(rec->preds[level])->lock);
        }
        if (!TOID_IS_NULL(rec->succs[level])) {
          pthread_mutex_unlock(&D_RW(rec->succs[level])->lock);
        }
      }
    }
    local_txlog->tx_status = ABORT;
    return -1;
  }
  TX_END

  // Release locks
  for (size_t i = 0; i < local_txlog->record_count; i++) {
    struct local_rec *rec = &local_txlog->records[i];
    for (int level = 0; level < MAX_LEVEL; level++) {
      if (!TOID_IS_NULL(rec->preds[level])) {
        pthread_mutex_unlock(&D_RW(rec->preds[level])->lock);
      }
      if (!TOID_IS_NULL(rec->succs[level])) {
        pthread_mutex_unlock(&D_RW(rec->succs[level])->lock);
      }
    }
  }

  local_txlog->tx_status = COMMIT;
  local_txlog->record_count = 0;

  return 0;
}

// Abort the transaction
void stm_abort(void) {
  if (local_txlog != NULL) {
    local_txlog->tx_status = ABORT;
    local_txlog->record_count = 0;
  }
}

// Generate random level for skip list
static int random_level(void) {
  int level = 1;
  while (((double)rand() / RAND_MAX) < SKIP_LIST_P && level < MAX_LEVEL) {
    level++;
  }
  return level;
}

// Skip list search function
static bool skip_search(int key, TOID(struct skip_node) preds[MAX_LEVEL],
                        TOID(struct skip_node) succs[MAX_LEVEL]) {
  TOID(struct skip_root) root = POBJ_ROOT(pop, struct skip_root);
  TOID(struct skip_node) pred = D_RO(root)->header;

  for (int level = MAX_LEVEL - 1; level >= 0; level--) {
    TOID(struct skip_node) curr = D_RO(pred)->forward[level];

    while (!TOID_IS_NULL(curr) && D_RO(curr)->key < key) {
      pred = curr;
      curr = D_RO(curr)->forward[level];
    }

    preds[level] = pred;
    succs[level] = curr;
  }

  return (!TOID_IS_NULL(succs[0]) && D_RO(succs[0])->key == key);
}

// Find node with specific key
static TOID(struct skip_node) skip_find(int key) {
  TOID(struct skip_node) preds[MAX_LEVEL];
  TOID(struct skip_node) succs[MAX_LEVEL];

  bool found = skip_search(key, preds, succs);

  if (found && !D_RO(succs[0])->marked) {
    return succs[0];
  }

  return TOID_NULL(struct skip_node);
}

// Actual insert operation (called during commit)
static int perform_insert(int key, int val) {
  TOID(struct skip_root) root = POBJ_ROOT(pop, struct skip_root);
  TOID(struct skip_node) preds[MAX_LEVEL];
  TOID(struct skip_node) succs[MAX_LEVEL];

  // Check if key already exists
  if (skip_search(key, preds, succs) && !D_RO(succs[0])->marked) {
    // Key exists, add new version
    TOID(struct version_list) vl = TX_ZNEW(struct version_list);
    TX_SET(vl, ts, local_txlog->tid);
    TX_SET(vl, val, val);
    TX_SET(vl, mark, false);
    TX_SET(vl, vnext, D_RO(succs[0])->vl);
    TX_SET(succs[0], vl, vl);
    return 0;
  }

  // Create new node
  int level = random_level();
  TOID(struct skip_node) new_node = TX_ZNEW(struct skip_node);
  TX_SET(new_node, key, key);
  TX_SET(new_node, level, level);
  TX_SET(new_node, marked, false);

  // Initialize mutex
  pthread_mutex_init(&D_RW(new_node)->lock, NULL);

  // Create version list
  TOID(struct version_list) vl = TX_ZNEW(struct version_list);
  TX_SET(vl, ts, local_txlog->tid);
  TX_SET(vl, val, val);
  TX_SET(vl, mark, false);
  TX_SET(new_node, vl, vl);

  // Update current level if necessary
  if (level > D_RO(root)->current_level) {
    TX_SET(root, current_level, level);
  }

  // Link the new node
  for (int i = 0; i < level; i++) {
    TX_SET_DIRECT(&D_RW(new_node)->forward[i], succs[i]);
    TX_SET_DIRECT(&D_RW(preds[i])->forward[i], new_node);
  }

  TX_ADD(root, node_count, 1);
  return 0;
}

// Actual delete operation (called during commit) - lazy deletion
static int perform_delete(int key) {
  TOID(struct skip_node) preds[MAX_LEVEL];
  TOID(struct skip_node) succs[MAX_LEVEL];

  if (!skip_search(key, preds, succs) || D_RO(succs[0])->marked) {
    return -1; // Key not found or already deleted
  }

  TOID(struct skip_node) node = succs[0];

  // Mark as deleted lazily
  TX_SET(node, marked, true);

  // Add deletion version to version list
  TOID(struct version_list) vl = TX_ZNEW(struct version_list);
  TX_SET(vl, ts, local_txlog->tid);
  TX_SET(vl, val, 0);
  TX_SET(vl, mark, true);
  TX_SET(vl, vnext, D_RO(node)->vl);
  TX_SET(node, vl, vl);

  return 0;
}

// Helper function implementations
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

  if (local_txlog->record_count >= local_txlog->record_capacity) {
    local_txlog->record_capacity *= 2;
    local_txlog->records =
        realloc(local_txlog->records,
                sizeof(struct local_rec) * local_txlog->record_capacity);
  }

  struct local_rec *rec = &local_txlog->records[local_txlog->record_count++];
  rec->obj_id = obj_id;
  rec->key = key;
  rec->val = 0;
  rec->pred = TOID_NULL(struct skip_node);
  rec->curr = TOID_NULL(struct skip_node);
  rec->node = TOID_NULL(struct skip_node);
  rec->op_status = OK;
  rec->opn = LOOKUP;

  // Initialize preds and succs arrays
  for (int i = 0; i < MAX_LEVEL; i++) {
    rec->preds[i] = TOID_NULL(struct skip_node);
    rec->succs[i] = TOID_NULL(struct skip_node);
  }

  return 0;
}

static void set_opn(int obj_id, int key, op_name_t op) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec)
    rec->opn = op;
}

static void set_val(int obj_id, int key, int val) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec)
    rec->val = val;
}

static void set_op_status(int obj_id, int key, status_t status) {
  struct local_rec *rec = find_record(obj_id, key);
  if (rec)
    rec->op_status = status;
}

static op_name_t get_opn(int obj_id, int key) {
  struct local_rec *rec = find_record(obj_id, key);
  return rec ? rec->opn : LOOKUP;
}

static int get_val(int obj_id, int key) {
  struct local_rec *rec = find_record(obj_id, key);
  return rec ? rec->val : 0;
}

static status_t get_op_status(int obj_id, int key) {
  struct local_rec *rec = find_record(obj_id, key);
  return rec ? rec->op_status : FAIL;
}

// Print skip list for debugging
void print_skip_list(void) {
  TOID(struct skip_root) root = POBJ_ROOT(pop, struct skip_root);
  TOID(struct skip_node) current = D_RO(D_RO(root)->header)->forward[0];

  printf("Skip List: ");
  while (!TOID_IS_NULL(current)) {
    if (!D_RO(current)->marked) {
      TOID(struct version_list) vl = D_RO(current)->vl;
      printf("(%d:%d) ", D_RO(current)->key,
             TOID_IS_NULL(vl) ? 0 : D_RO(vl)->val);
    }
    current = D_RO(current)->forward[0];
  }
  printf("\n");
}

// Cleanup function
void stm_cleanup(void) {
  if (local_txlog) {
    free(local_txlog->records);
    free(local_txlog);
    local_txlog = NULL;
  }

  if (pop) {
    pmemobj_close(pop);
    pop = NULL;
  }
}

// Example usage
int main() {
  if (stm_init() != 0) {
    fprintf(stderr, "Failed to initialize STM\n");
    return 1;
  }

  // Begin transaction
  int tid = stm_begin();
  printf("Started transaction %d\n", tid);

  // Insert some values
  stm_insert(1, 10, 100);
  stm_insert(1, 20, 200);
  stm_insert(1, 30, 300);
  stm_insert(1, 5, 50);

  // Lookup
  int val;
  status_t status;
  stm_lookup(1, 20, &val, &status);
  printf("Lookup key 20: val=%d, status=%d\n", val, status);

  // Delete
  stm_delete(1, 20);

  // Another lookup after delete
  stm_lookup(1, 20, &val, &status);
  printf("Lookup key 20 after delete: val=%d, status=%d\n", val, status);

  // Try to commit
  if (stm_try_commit() == 0) {
    printf("Transaction committed successfully\n");
    print_skip_list();
  } else {
    printf("Transaction aborted\n");
  }

  // Test persistence - start new transaction
  printf("\nStarting new transaction to test persistence...\n");
  tid = stm_begin();

  // Lookup values from previous transaction
  stm_lookup(1, 10, &val, &status);
  printf("Lookup key 10: val=%d, status=%d\n", val, status);

  stm_lookup(1, 30, &val, &status);
  printf("Lookup key 30: val=%d, status=%d\n", val, status);

  stm_try_commit();

  stm_cleanup();
  return 0;
}