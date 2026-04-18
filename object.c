// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions: object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/sha.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(id_out->hash, &ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // Step 1: Build the header string
    const char *type_str;
    switch (type) {
        case OBJ_BLOB:   type_str = "blob";   break;
        case OBJ_TREE:   type_str = "tree";   break;
        case OBJ_COMMIT: type_str = "commit"; break;
        default: return -1;
    }

    // Header: "<type> <size>\0"
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;
    // +1 to include the null terminator in header_len

    // Step 2: Build the full object (header + data) in a single buffer
    size_t full_len = (size_t)header_len + len;
    uint8_t *full_obj = malloc(full_len);
    if (!full_obj) return -1;
    memcpy(full_obj, header, (size_t)header_len);
    memcpy(full_obj + header_len, data, len);

    // Step 3: Compute SHA-256 of the full object
    ObjectID id;
    compute_hash(full_obj, full_len, &id);

    // Step 4: Check deduplication — if already stored, just return success
    if (object_exists(&id)) {
        if (id_out) *id_out = id;
        free(full_obj);
        return 0;
    }

    // Step 5: Create shard directory (.pes/objects/XX/) if needed
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);

    char shard_dir[512];
    snprintf(shard_dir, sizeof(shard_dir), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_dir, 0755); // ignore error if already exists

    // Step 6: Write to a temporary file in the shard directory
    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));

    char tmp_path[560];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", final_path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) {
        free(full_obj);
        return -1;
    }

    ssize_t written = write(fd, full_obj, full_len);
    free(full_obj);

    if (written < 0 || (size_t)written != full_len) {
        close(fd);
        unlink(tmp_path);
        return -1;
    }

    // Step 7: fsync to ensure data hits disk
    fsync(fd);
    close(fd);

    // Step 8: Atomically rename temp file to final path
    if (rename(tmp_path, final_path) != 0) {
        unlink(tmp_path);
        return -1;
    }

    // Step 9: fsync the shard directory to persist the rename
    int dir_fd = open(shard_dir, O_RDONLY);
    if (dir_fd >= 0) {
        fsync(dir_fd);
        close(dir_fd);
    }

    // Store result
    if (id_out) *id_out = id;
    return 0;
}

// Read an object from the store.
//
// Returns 0 on success, -1 on error.
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // Step 1: Get the file path
    char path[512];
    object_path(id, path, sizeof(path));

    // Step 2: Read the entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (file_size <= 0) {
        fclose(f);
        return -1;
    }

    uint8_t *raw = malloc((size_t)file_size);
    if (!raw) {
        fclose(f);
        return -1;
    }

    if (fread(raw, 1, (size_t)file_size, f) != (size_t)file_size) {
        fclose(f);
        free(raw);
        return -1;
    }
    fclose(f);

    // Step 3: Verify integrity — recompute hash and compare to filename
    ObjectID computed;
    compute_hash(raw, (size_t)file_size, &computed);
    if (memcmp(computed.hash, id->hash, HASH_SIZE) != 0) {
        free(raw);
        return -1; // Corrupt object
    }

    // Step 4: Parse the header to find the null separator
    uint8_t *null_pos = memchr(raw, '\0', (size_t)file_size);
    if (!null_pos) {
        free(raw);
        return -1;
    }

    // Header is everything before the '\0'
    size_t header_len = (size_t)(null_pos - raw);
    char header[128];
    if (header_len >= sizeof(header)) {
        free(raw);
        return -1;
    }
    memcpy(header, raw, header_len);
    header[header_len] = '\0';

    // Parse type string and size from header
    char type_str[16];
    size_t data_size;
    if (sscanf(header, "%15s %zu", type_str, &data_size) != 2) {
        free(raw);
        return -1;
    }

    // Step 5: Set type_out
    if (strncmp(type_str, "blob", 4) == 0)        *type_out = OBJ_BLOB;
    else if (strncmp(type_str, "tree", 4) == 0)   *type_out = OBJ_TREE;
    else if (strncmp(type_str, "commit", 6) == 0) *type_out = OBJ_COMMIT;
    else {
        free(raw);
        return -1;
    }

    // Step 6: Allocate and copy out the data portion (after the '\0')
    uint8_t *data_start = null_pos + 1;
    size_t remaining = (size_t)file_size - header_len - 1;
    if (remaining != data_size) {
        free(raw);
        return -1;
    }

    void *out = malloc(data_size + 1); // +1 for safe null termination
    if (!out) {
        free(raw);
        return -1;
    }
    memcpy(out, data_start, data_size);
    ((uint8_t *)out)[data_size] = '\0'; // safe terminator

    free(raw);
    *data_out = out;
    *len_out = data_size;
    return 0;
}
/* Phase 1: object store complete - supports blob, tree, commit types */
/* Phase 1: deduplication verified - same content produces same hash */
/* Phase 1: atomic write uses temp+rename pattern for crash safety */
