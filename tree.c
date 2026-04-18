// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions: tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"

#include "tree.h"
#include "index.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE 0100644
#define MODE_EXEC 0100755
#define MODE_DIR  0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;
    if (S_ISDIR(st.st_mode)) return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1;

        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);
        ptr = space + 1;

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1;

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0';
        ptr = null_byte + 1;

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1;
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    size_t max_size = tree->count * 296;
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to include the null terminator
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Forward declaration for object_write (implemented in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// Helper: write a tree level for a subset of index entries that share a
// common directory prefix. `prefix` is the directory path prefix (e.g. "src/"),
// and `prefix_len` is its length. We only look at entries[start..start+count).
//
// Strategy: iterate entries. For each entry:
//   - Strip the prefix from the path.
//   - If the remaining name has no '/', it's a file in this directory -> add blob entry.
//   - If the remaining name has a '/', it's in a subdirectory -> collect all entries
//     belonging to that subdirectory and recurse.
static int write_tree_level(IndexEntry *entries, int count,
                             const char *prefix, size_t prefix_len,
                             ObjectID *id_out) {
    Tree tree;
    tree.count = 0;

    int i = 0;
    while (i < count) {
        const char *full_path = entries[i].path;
        // Strip the prefix
        const char *rel = full_path + prefix_len;

        // Find the next '/' in the relative path
        const char *slash = strchr(rel, '/');

        if (!slash) {
            // This is a direct file in this directory
            TreeEntry *te = &tree.entries[tree.count++];
            te->mode = entries[i].mode;
            te->hash = entries[i].hash;
            strncpy(te->name, rel, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';
            i++;
        } else {
            // This entry is in a subdirectory. The subdirectory name is rel[0..slash-rel)
            size_t subdir_name_len = (size_t)(slash - rel);
            char subdir_name[256];
            if (subdir_name_len >= sizeof(subdir_name)) return -1;
            memcpy(subdir_name, rel, subdir_name_len);
            subdir_name[subdir_name_len] = '\0';

            // Build the new prefix for the recursive call
            char new_prefix[1024];
            snprintf(new_prefix, sizeof(new_prefix), "%.*s%s/",
                     (int)prefix_len, prefix, subdir_name);
            size_t new_prefix_len = prefix_len + subdir_name_len + 1;

            // Collect all entries that share this new prefix
            int j = i;
            while (j < count &&
                   strncmp(entries[j].path, new_prefix, new_prefix_len) == 0) {
                j++;
            }

            // Recurse for the subdirectory
            ObjectID sub_id;
            if (write_tree_level(entries + i, j - i,
                                  new_prefix, new_prefix_len, &sub_id) != 0) {
                return -1;
            }

            // Add a tree entry for this subdirectory
            TreeEntry *te = &tree.entries[tree.count++];
            te->mode = MODE_DIR;
            te->hash = sub_id;
            strncpy(te->name, subdir_name, sizeof(te->name) - 1);
            te->name[sizeof(te->name) - 1] = '\0';

            i = j;
        }
    }

    // Serialize and store this tree level
    void *tree_data;
    size_t tree_len;
    if (tree_serialize(&tree, &tree_data, &tree_len) != 0) return -1;

    int rc = object_write(OBJ_TREE, tree_data, tree_len, id_out);
    free(tree_data);
    return rc;
}

// Comparison function for sorting index entries by path (for tree building)
static int compare_index_entries_by_path(const void *a, const void *b) {
    return strcmp(((const IndexEntry *)a)->path, ((const IndexEntry *)b)->path);
}

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    Index index;
    if (index_load(&index) != 0) return -1;

    if (index.count == 0) {
        // Empty index: write an empty tree
        Tree empty_tree;
        empty_tree.count = 0;
        void *tree_data;
        size_t tree_len;
        if (tree_serialize(&empty_tree, &tree_data, &tree_len) != 0) return -1;
        int rc = object_write(OBJ_TREE, tree_data, tree_len, id_out);
        free(tree_data);
        return rc;
    }

    // Sort entries by path so subdirectory grouping works correctly
    qsort(index.entries, index.count, sizeof(IndexEntry), compare_index_entries_by_path);

    // Build the root tree level (prefix = "", prefix_len = 0)
    return write_tree_level(index.entries, index.count, "", 0, id_out);
}
/* Phase 2: tree entries sorted by name for deterministic hashing */
