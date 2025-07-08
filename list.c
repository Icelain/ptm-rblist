
#include <libpmemobj.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LAYOUT_NAME "mvostm_red_blue_list"
#define POOL_SIZE (1024 * 1024 * 20)

POBJ_LAYOUT_BEGIN(mvostm_red_blue_list);
POBJ_LAYOUT_ROOT(mvostm_red_blue_list, struct rb_list);
POBJ_LAYOUT_TOID(mvostm_red_blue_list, struct rb_node);
POBJ_LAYOUT_TOID(mvostm_red_blue_list, struct version);
POBJ_LAYOUT_TOID(mvostm_red_blue_list, struct tx_record);
POBJ_LAYOUT_END(mvostm_red_blue_list);

struct version {
  uint64_t ts;
  int val;
  bool marked;
  TOID(struct version) vnext;
};

struct rb_node {
  int key;
  bool lock;
  bool marked;
  TOID(struct version) vl;
  TOID(struct rb_node) bl_next;
  TOID(struct rb_node) rl_next;
};

struct tx_record {
  uint64_t ts;
  int key;
  int val;
  int type; // 0 = insert, 1 = delete, 2 = lookup
  TOID(struct tx_record) next;
};

struct rb_list {
  TOID(struct rb_node) head;
  uint64_t tx_counter;
};

uint64_t get_new_ts(PMEMobjpool *pop) {
  TOID(struct rb_list) root = POBJ_ROOT(pop, struct rb_list);
  return ++(D_RW(root)->tx_counter);
}

// Create a new version
TOID(struct version)
create_version(PMEMobjpool *pop, uint64_t ts, int val, bool marked) {
  TOID(struct version) v = TX_NEW(struct version);
  D_RW(v)->ts = ts;
  D_RW(v)->val = val;
  D_RW(v)->marked = marked;
  D_RW(v)->vnext = TOID_NULL(struct version);
  return v;
}

// Create a new node
TOID(struct rb_node)
create_node(PMEMobjpool *pop, int key, TOID(struct version) version) {
  TOID(struct rb_node) node = TX_NEW(struct rb_node);
  D_RW(node)->key = key;
  D_RW(node)->marked = false;
  D_RW(node)->vl = version;
  D_RW(node)->bl_next = TOID_NULL(struct rb_node);
  D_RW(node)->rl_next = TOID_NULL(struct rb_node);
  return node;
}

void rb_list_init(PMEMobjpool *pop) {
  TOID(struct rb_list) root = POBJ_ROOT(pop, struct rb_list);
  TX_BEGIN(pop) {
    TOID(struct version) head_ver = create_version(pop, 0, -__INT_MAX__, false);
    TOID(struct version) tail_ver = create_version(pop, 0, __INT_MAX__, false);

    TOID(struct rb_node) head = create_node(pop, -__INT_MAX__, head_ver);
    TOID(struct rb_node) tail = create_node(pop, __INT_MAX__, tail_ver);

    D_RW(head)->bl_next = tail;
    D_RW(head)->rl_next = tail;

    D_RW(root)->head = head;
    D_RW(root)->tx_counter = 1;
  }
  TX_END
}

TOID(struct version) find_lts_version(TOID(struct rb_node) node, uint64_t ts) {
  TOID(struct version) curr = D_RO(node)->vl;
  while (!TOID_IS_NULL(curr)) {
    if (D_RO(curr)->ts < ts)
      return curr;
    curr = D_RO(curr)->vnext;
  }
  return TOID_NULL(struct version);
}

int t_lookup(PMEMobjpool *pop, uint64_t ts, int key) {
  TOID(struct rb_list) root = POBJ_ROOT(pop, struct rb_list);
  TOID(struct rb_node) curr = D_RO(root)->head;
  while (!TOID_IS_NULL(curr)) {
    if (D_RO(curr)->key == key && !D_RO(curr)->marked) {
      TOID(struct version) v = find_lts_version(curr, ts);
      if (!TOID_IS_NULL(v) && !D_RO(v)->marked)
        return D_RO(v)->val;
      else
        return -1; // not found
    }
    curr = D_RO(curr)->bl_next;
  }
  return -1;
}

void t_insert(PMEMobjpool *pop, uint64_t ts, int key, int val) {
  TOID(struct rb_list) root = POBJ_ROOT(pop, struct rb_list);
  TX_BEGIN(pop) {
    TOID(struct rb_node) pred = D_RW(root)->head;
    TOID(struct rb_node) curr = D_RO(pred)->bl_next;

    while (!TOID_IS_NULL(curr) && D_RO(curr)->key < key) {
      pred = curr;
      curr = D_RO(curr)->bl_next;
    }

    TOID(struct version) ver = create_version(pop, ts, val, false);
    if (!TOID_IS_NULL(curr) && D_RO(curr)->key == key) {
      D_RW(ver)->vnext = D_RO(curr)->vl;
      D_RW(curr)->vl = ver;
    } else {
      TOID(struct rb_node) new_node = create_node(pop, key, ver);
      D_RW(new_node)->bl_next = curr;
      D_RW(new_node)->rl_next = D_RO(pred)->rl_next;
      D_RW(pred)->bl_next = new_node;
      D_RW(pred)->rl_next = new_node;
    }
  }
  TX_END
}

void t_delete(PMEMobjpool *pop, uint64_t ts, int key) {
  TOID(struct rb_list) root = POBJ_ROOT(pop, struct rb_list);
  TX_BEGIN(pop) {
    TOID(struct rb_node) pred = D_RW(root)->head;
    TOID(struct rb_node) curr = D_RO(pred)->bl_next;

    while (!TOID_IS_NULL(curr) && D_RO(curr)->key < key) {
      pred = curr;
      curr = D_RO(curr)->bl_next;
    }

    if (!TOID_IS_NULL(curr) && D_RO(curr)->key == key) {
      TOID(struct version) ver = create_version(pop, ts, 0, true);
      D_RW(ver)->vnext = D_RO(curr)->vl;
      D_RW(curr)->vl = ver;
      D_RW(curr)->marked = true;
      D_RW(pred)->bl_next = D_RO(curr)->bl_next;
    }
  }
  TX_END
}

void rb_list_print(PMEMobjpool *pop) {
  TOID(struct rb_list) root = POBJ_ROOT(pop, struct rb_list);
  TOID(struct rb_node) curr = D_RO(root)->head;

  printf("[BL] ");
  while (!TOID_IS_NULL(curr)) {
    printf("%d ", D_RO(curr)->key);
    curr = D_RO(curr)->bl_next;
  }
  printf("\n[RL] ");
  curr = D_RO(root)->head;
  while (!TOID_IS_NULL(curr)) {
    printf("%d%s ", D_RO(curr)->key, D_RO(curr)->marked ? "(x)" : "");
    curr = D_RO(curr)->rl_next;
  }
  printf("\n");
}

int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <pool_file>\n", argv[0]);
    return 1;
  }

  PMEMobjpool *pop;
  if (access(argv[1], F_OK) != 0) {
    pop = pmemobj_create(argv[1], LAYOUT_NAME, POOL_SIZE, 0666);
    if (!pop) {
      perror("pmemobj_create");
      return 1;
    }
    rb_list_init(pop);
  } else {
    pop = pmemobj_open(argv[1], LAYOUT_NAME);
    if (!pop) {
      perror("pmemobj_open");
      return 1;
    }
  }

  uint64_t ts1 = get_new_ts(pop);
  t_insert(pop, ts1, 10, 100);

  uint64_t ts2 = get_new_ts(pop);
  t_insert(pop, ts2, 20, 200);

  uint64_t ts3 = get_new_ts(pop);
  t_delete(pop, ts3, 10);

  rb_list_print(pop);

  pmemobj_close(pop);
  return 0;
}