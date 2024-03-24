//
// Starter code for CS 454/654
// You SHOULD change this file
//

#include "rpc.h"
#include "debug.h"
INIT_LOG

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <cstdlib>
#include <fuse.h>
#include <map>
#include "rw_lock.h"
#include <string>
#include <vector>
#include <cstdarg>
#include <cerrno>
#include <iostream>

// Global state server_persist_dir.
char *server_persist_dir = nullptr;
rw_lock_t *file_lock;

enum AccessType {
    READ,
    WRITE
};

// Important: the server needs to handle multiple concurrent client requests.
// You have to be careful in handling global variables, especially for updating them.
// Hint: use locks before you update any global variable.
struct file_handler {
    AccessType access_type;
    rw_lock_t * lock;
};

void debug(const char* format, ...) {
    va_list args;
    va_start(args, format);

    // We need to safely determine the size of the formatted string.
    // vsnprintf returns the number of characters (excluding the null terminator) 
    // that would have been written if the buffer was large enough.
    int size = vsnprintf(nullptr, 0, format, args) + 1; // Adding one for the null terminator
    va_end(args);

    if (size <= 0) {
        std::cerr << "Error in formatting the debug log message." << std::endl;
        return;
    }

    // Create a vector with the required size to hold the formatted string.
    std::vector<char> buf(size);

    va_start(args, format);
    vsnprintf(buf.data(), size, format, args);
    va_end(args);

    // Output the formatted string to std::cerr
    std::cerr << buf.data() << std::endl;
}

std::map<std::string, struct file_handler> files;

// We need to operate on the path relative to the server_persist_dir.
// This function returns a path that appends the given short path to the
// server_persist_dir. The character array is allocated on the heap, therefore
// it should be freed after use.
// Tip: update this function to return a unique_ptr for automatic memory management.
char *get_full_path(char *short_path) {
    int short_path_len = strlen(short_path);
    int dir_len = strlen(server_persist_dir);
    int full_len = dir_len + short_path_len + 1;

    char *full_path = (char *)malloc(full_len);

    // First fill in the directory.
    strcpy(full_path, server_persist_dir);
    // Then append the path.
    strcat(full_path, short_path);
    debug("Full path: %s\n", full_path);

    return full_path;
}

// The server implementation of getattr.
int watdfs_getattr(int *argTypes, void **args) {
    // Get the arguments.
    // The first argument is the path relative to the mountpoint.
    char *short_path = (char *)args[0];
    // The second argument is the stat structure, which should be filled in
    // by this function.
    struct stat *statbuf = (struct stat *)args[1];
    // The third argument is the return code, which should be set be 0 or -errno.
    int *ret = (int *)args[2];

    // Get the local file name, so we call our helper function which appends
    // the server_persist_dir to the given path.
    char *full_path = get_full_path(short_path);

    // Initially we set the return code to be 0.
    *ret = 0;

    // TODO: Make the stat system call, which is the corresponding system call needed (done)
    // to support getattr. You should use the statbuf as an argument to the stat system call.
    // Let syscall be the return code from the stat system call.
    int syscall = stat(full_path, statbuf);

    if (syscall < 0) {
        // If there is an error on the system call, then the return code should
        // be -errno.
        *ret = -errno;
        debug("getattr failed with code '%d'", *ret);

    }

    // Clean up the full path, it was allocated on the heap.
    free(full_path);

    //debug("Returning code: %d", *ret);
    // The RPC call succeeded, so return 0.
    return 0;
}

int watdfs_mknod(int *argTypes, void** args) {
    // Unpack short path
    char *short_path = (char*)args[0];
    // Unpack mode
    mode_t* mode = (mode_t*) args[1];
    // Unpack dev
    dev_t* dev = (dev_t*) args[2];
    // Unpack return value
    int *ret = (int *)args[3];
    // Set ret to 0 intially
    *ret = 0;
    // Get full path
    char *full_path = get_full_path(short_path);
    // Call the mknod syscall
    int syscall = mknod(full_path, *mode, *dev);
    // If we found an error, update return code
    if (syscall < 0) {
        *ret = -errno;
        debug("mknod failed with code '%d'", *ret);
    } else {
        *ret = syscall;
    }

    free(full_path);

    return syscall;
}

int watdfs_open(int *argTypes, void **args) {
    // Unpack short path
    char *short_path = (char*)args[0];
    // Unpack file info
    struct fuse_file_info *fi = (struct fuse_file_info*)args[1];
    // Unpack return value
    int *ret = (int*)args[2];

    *ret = 0;

    // Get full path
    char *full_path = get_full_path(short_path);

    std::string short_path_string = std::string(short_path);
    rw_lock_lock(file_lock, RW_WRITE_LOCK);
    
    // If file is not open
    if (files.find(short_path_string) == files.end()) {
        AccessType new_access_type = fi->flags == O_RDONLY ? READ : WRITE;
        // Create a new struct for this file
        struct file_handler file_data = { new_access_type, new rw_lock_t };
        rw_lock_init(file_data.lock);
        files[short_path_string] = file_data;
    } else {
        // The file is already open
        if (files[short_path_string].access_type == READ) {
            // File is already open in read mode
            if (fi->flags == O_RDWR) files[short_path_string].access_type = WRITE;
        } else {
            // File is already open in write mode
            rw_lock_unlock(file_lock, RW_WRITE_LOCK);
            if (fi->flags == O_RDWR) return -EACCES;
        }
    }
    rw_lock_unlock(file_lock, RW_WRITE_LOCK);

    // Open file
    debug("full_path: '%s', flags: '%d'", full_path, fi->flags);
    int syscall = open(full_path, fi->flags);

    if(syscall < 0) {
        *ret = -errno;
        debug("open failed with code '%d'", *ret);
    } else {
        // Set file handle
        debug("syscall is: '%d'", syscall);
        fi->fh = syscall;
        *ret = 0;
        debug("new file handle: '%ld'", fi->fh);
    }

    // Free full path
    free(full_path);

    return syscall;
}

int watdfs_release(int *argTypes, void** args) {
    // Unpack short path
    char *short_path = (char*)args[0];
    // Unpack fuze file info
    struct fuse_file_info *fi = (struct fuse_file_info*)args[1];
    // Unpack return code
    int *ret = (int*)args[2];

    *ret = 0;

    // Get full path
    char *full_path = get_full_path(short_path);

    debug("closing file with fh '%ld'", fi->fh);

    int syscall = close(fi->fh);

    if (syscall < 0) {
        *ret = -errno;
        debug("close failed with code '%d'", *ret);

    } else {
        *ret = syscall;
        rw_lock_lock(file_lock, RW_WRITE_LOCK);
        files.erase(std::string(full_path));
        rw_lock_unlock(file_lock, RW_WRITE_LOCK);
    }

    free(full_path);
    return 0;
}

int watdfs_fsync(int *argTypes, void **args) {
    // Unpack short path
    char *short_path = (char*) args[0];
    // Unpack fuse file info
    struct fuse_file_info *fi = (struct fuse_file_info*)args[1];
    // Unpack return code;
    int *ret = (int* )args[2];

    *ret = 0;

    char* full_path = get_full_path(short_path);
    // Make fsync syscall
    int syscall = fsync(fi->fh);

    if (syscall < 0) {
        *ret = -errno;
        debug("fsync failed with code '%d'", syscall);

    } else {
        *ret = syscall;
    }

    free(full_path);

    return 0;
}

int watdfs_utimensat(int* argTypes, void **args) {
    // Unpack short path
    char *short_path = (char*) args[0];\
    // Unpack fuse file info
    struct timespec* ts = (struct timespec*)args[1];
    // Unpack return code;
    int *ret = (int* )args[2];
    *ret = 0;

    char *full_path = get_full_path(short_path);
    // Make fsync syscall
    int syscall = utimensat(0, full_path, ts, 0);

    if (syscall < 0) {
        *ret = -errno;
        debug("utimensat failed with code '%d'", syscall);

    } else {
        *ret = syscall;
    }

    free(full_path);

    return 0;
}

int watdfs_read(int* argTypes, void** args)  {
    // Unpack short path
    char *short_path = (char*)args[0];
    // Unpack buffer pointer
    char *buf = (char*)args[1];
    // Unpack size
    size_t *size = (size_t*)args[2];
    // Unpack offset 
    off_t *off = (off_t*)args[3];
    // Unpack file handler
    fuse_file_info *fi = (struct fuse_file_info*)args[4];
    // Unpack return code
    int *ret = (int*) args[5];
    // Get full path
    char *full_path = get_full_path(short_path);

    *ret = 0;
    int syscall = pread(fi->fh, buf, *size, *off);

    debug("reading file with fh '%ld'", fi->fh);
    debug("requesting to read '%ld' bytes", *size);

    if (syscall < 0) {
        *ret = -errno;
        debug("read failed with code '%d'", syscall);
    } else {
        *ret = syscall;
        debug("successfully read '%d' bytes", syscall);
    }

    free(full_path);
    return *ret;
}

int watdfs_write(int *argTypes, void**args) {
     // Unpack short path
    char *short_path = (char*)args[0];
    // Unpack buffer pointer
    char *buf = (char*)args[1];
    // Unpack size
    size_t *size = (size_t*)args[2];
    // Unpack offset 
    off_t *off = (off_t*)args[3];
    // Unpack file handler
    fuse_file_info *fi = (struct fuse_file_info*)args[4];
    // Unpack return code
    int *ret = (int*) args[5];

    char *full_path = get_full_path(short_path);
    *ret = 0;

    debug("writing file with fh '%ld'", fi->fh);

    int syscall = pwrite(fi->fh, buf, *size, *off);

    if (syscall < 0) {
        *ret = -errno;
        debug("write failed with code '%d'", syscall);
    } else {
        *ret = syscall;
    }
    free(full_path);
    return *ret;
}

int watdfs_truncate(int* argTypes, void**args) {
    // Unpack short path
    char *short_path = (char*)args[0];
    // Unpack new size
    off_t *new_size = (off_t*)args[1];
    // Unpack return var
    int *ret = (int*)args[2];

    char *full_path = get_full_path(short_path);
    *ret = 0;

    int syscall = truncate(full_path, *new_size);

    if (syscall < 0) {
        *ret = -errno;
        debug("truncate failed with code '%d'", syscall);

    } else {
        *ret = syscall;
    }

    free(full_path);
    return 0;
}

int watdfs_lock(int *argTypes, void**args) {
    char *short_path = (char*)args[0];
    char *full_path = get_full_path(short_path);

    rw_lock_mode_t *mode = (rw_lock_mode_t* )args[1];

    int *ret = (int*)args[2];

    std::string path_string = std::string(short_path);

    rw_lock_lock(file_lock, RW_WRITE_LOCK);
    if (files.find(path_string) == files.end()) {
        AccessType new_access_type = *mode == RW_READ_LOCK ? READ : WRITE;
        // Create a new struct for this file
        struct file_handler file_data = { new_access_type, new rw_lock_t };
        rw_lock_init(file_data.lock);
        files[path_string] = file_data;
    }
    rw_lock_unlock(file_lock, RW_WRITE_LOCK);

    rw_lock_t *lock = files[path_string].lock;

    int lock_ret = rw_lock_lock(lock, *mode);
    if (lock_ret < 0) {
        *ret = -1;
    } else {
        *ret = lock_ret;
    }

    free(full_path);
    return 0;
}

int watdfs_unlock(int* argTypes, void**args) {
    char *short_path = (char*)args[0];
    char *full_path = get_full_path(short_path);

    rw_lock_mode_t *mode = (rw_lock_mode_t* )args[1];

    int *ret = (int*)args[2];

    rw_lock_lock(file_lock, RW_WRITE_LOCK);
    rw_lock_t *lock = files[std::string(short_path)].lock;
    int unlock_ret = rw_lock_unlock(lock, *mode);
    rw_lock_unlock(file_lock, RW_WRITE_LOCK);

    debug("unlock return value: %d", unlock_ret);
    if (unlock_ret < 0) {
        *ret = -1;
    } else {
        *ret = unlock_ret;
    }

    free(full_path);
    return 0;
}

// The main function of the server.
int main(int argc, char *argv[]) {
    // argv[1] should contain the directory where you should store data on the
    // server. If it is not present it is an error, that we cannot recover from.
    if (argc != 2) {
        // In general, you shouldn't print to stderr or stdout, but it may be
        // helpful here for debugging. Important: Make sure you turn off logging
        // prior to submission!
        // See watdfs_client.cpp for more details
        // # ifdef PRINT_ERR
        // std::cerr << "Usage:" << argv[0] << " server_persist_dir";
        // #endif
        return -1;
    }
    // Store the directory in a global variable.
    server_persist_dir = argv[1];

    int setup_code = rpcServerInit();
    rw_lock_init(file_lock);

    if (setup_code < 0) {
        debug("rpcServerInit failed with '%d'", setup_code);
        return setup_code;
    }

    // TODO: Initialize the rpc library by calling `rpcServerInit`.
    // Important: `rpcServerInit` prints the 'export SERVER_ADDRESS' and
    // 'export SERVER_PORT' lines. Make sure you *do not* print anything
    // to *stdout* before calling `rpcServerInit`.
    //debug("Initializing server...");

    int ret = 0;
    // TODO: If there is an error with `rpcServerInit`, it maybe useful to have
    // debug-printing here, and then you should return.

    // TODO: Register your functions with the RPC library.
    // Note: The braces are used to limit the scope of `argTypes`, so that you can
    // reuse the variable for multiple registrations. Another way could be to
    // remove the braces and use `argTypes0`, `argTypes1`, etc.

    /* getattr registration */
    {
        // There are 3 args for the function (see watdfs_client.cpp for more
        // detail).
        int argTypes[4];
        // First is the path.
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // The second argument is the statbuf.
        argTypes[1] =
            (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // The third argument is the retcode.
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // Finally we fill in the null terminator.
        argTypes[3] = 0;

        // We need to register the function with the types and the name.
        ret = rpcRegister((char *)"getattr", argTypes, watdfs_getattr);
        if (ret < 0) {
            // It may be useful to have debug-printing here.
            debug("rpcRegister failed to register getattr with '%d'", ret);
            return ret;
        }
    }

    /* mknod registration */
    {   
        // mknod argTypes
        int argTypes[5];

        // First argument is path
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // Second argument is mode
        argTypes[1] = (1u << ARG_INPUT) | (ARG_INT << 16u);

        // Third argument is dev
        argTypes[2] = (1u << ARG_INPUT) | (ARG_LONG << 16u);

        // Fourth argument is return
        argTypes[3] = (1u << ARG_OUTPUT)  | (ARG_INT << 16u);

        argTypes[4] = 0;

        // Register function types and name
        ret = rpcRegister((char *)"mknod", argTypes, watdfs_mknod);
        if (ret < 0) {
            debug("rpcRegister failed to register mknod with '%d'", ret);
            return ret;
        }
    }

    /* open registration */
    {
        // open argTypes
        int argTypes[4];

        // First is the path.
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // The second argument is the file info.
        argTypes[1] =
            (1u << ARG_INPUT) | (1u << ARG_OUTPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u);
        // The third argument is the retcode.
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // Finally we fill in the null terminator.
        argTypes[3] = 0;

        // We need to register the function with the types and the name.
        ret = rpcRegister((char *)"open", argTypes, watdfs_open);
        if (ret < 0) {
            // It may be useful to have debug-printing here.
            debug("rpcRegister failed to register open with '%d'", ret);
            return ret;
        }
    }

    /* release registration */
    {
        // release argTypes
        int argTypes[4];

        // First is the path.
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // The second argument is the file info.
        argTypes[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u);
        // The third argument is the retcode.
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // Finally we fill in the null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char* )"release", argTypes, watdfs_release);
        if (ret < 0) {
            debug("rpcRegister failed to register release with '%d'", ret);
        }
    }

    /* fsync registration */ 
    {
        int argTypes[4];

        // First is path
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // Second is file info
        argTypes[1] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u);
        // Third is retcode
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        // Finally we fill in the null terminator.
        argTypes[3] = 0;

        ret = rpcRegister((char* )"fsync", argTypes, watdfs_fsync);
        if(ret < 0) {
            debug("rpcRegister failed to register fsync with '%d'", ret);
        }
    }

    /* utimensat registration */
    {
        int argTypes[4];

        // First is path
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // Setting second argument to input, array, and char
        argTypes[1] = (1u << ARG_INPUT) | (1u << ARG_ARRAY)| (ARG_CHAR << 16u) | 1u;
        
        // Last arg is return
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);

        argTypes[3] = 0;

        ret = rpcRegister((char *)"utimensat", argTypes, watdfs_utimensat);
        if(ret < 0) {
            debug("rpcRegister failed to register utimensat with '%d'", ret);
        }
    }

    /* Read registration */
    {
        int argTypes[7];

        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
            // Buffer type and size
        argTypes[1] = 
            (1u << ARG_OUTPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
            // Size type
        argTypes[2] = 
            (1u << ARG_INPUT) | (ARG_LONG << 16u);
            // Offset type
        argTypes[3] = 
            (1u << ARG_INPUT) | (ARG_LONG << 16u);
        // File handler type 
        argTypes[4] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        argTypes[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        argTypes[6] = 0;

        ret = rpcRegister((char* )"read", argTypes, watdfs_read);
        if(ret < 0) {
            debug("rpcRegister failed to register read with '%d'", ret);
        }
    } 
    
    /* Write registration */
    {
        int argTypes[7];

        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
            // Buffer type and size
        argTypes[1] = 
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
            // Size type
        argTypes[2] = 
            (1u << ARG_INPUT) | (ARG_LONG << 16u);
            // Offset type
        argTypes[3] = 
            (1u << ARG_INPUT) | (ARG_LONG << 16u);
        // File handler type 
        argTypes[4] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        // Return code type
        argTypes[5] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        argTypes[6] = 0;

        ret = rpcRegister((char* )"write", argTypes, watdfs_write);
        if(ret < 0) {
            debug("rpcRegister failed to register write with '%d'", ret);
        }
    } 

    /* Truncate registration */
    {
        int argTypes[4];
        argTypes[0] =
            (1u << ARG_INPUT) | (1u << ARG_ARRAY) | (ARG_CHAR << 16u) | 1u;
        argTypes[1] = (1u << ARG_INPUT) | (ARG_LONG << 16u);
        argTypes[2] = (1u << ARG_OUTPUT) | (ARG_INT << 16u);
        argTypes[3] = 0;

        ret = rpcRegister((char* )"truncate", argTypes, watdfs_truncate);
        if(ret < 0) {
            debug("rpcRegister failed to register truncate with '%d'", ret);
        }
    }

    /* Lock registration */
    {
        int argTypes[3];
        argTypes[0] = (1 << ARG_INPUT) | (1 << ARG_ARRAY) | (ARG_CHAR << 16) | 1u;
        argTypes[1] = (1 << ARG_INPUT) | (ARG_INT << 16);
        argTypes[2] = (1 << ARG_OUTPUT) | (ARG_INT << 16);
        argTypes[3] = 0;

        ret = rpcRegister((char*)"lock", argTypes, watdfs_lock);
        if (ret < 0) {
            debug("rpcRegister failed to register lock with '%d'", ret);
        }
    }

    /* Unlock registration*/
    {

        int argTypes[4];
        argTypes[0] = (1 << ARG_INPUT) | (1 << ARG_ARRAY) | (ARG_CHAR << 16) | 1u;
        argTypes[1] = (1 << ARG_INPUT) | (ARG_INT << 16);
        argTypes[2] = (1 << ARG_OUTPUT) | (ARG_INT << 16);
        argTypes[3] = 0;

        ret = rpcRegister((char*)"unlock", argTypes, watdfs_unlock);
        if (ret < 0) {
            debug("rpcRegister failed to register unlock with '%d'", ret);
        }
    }

    // TODO: Hand over control to the RPC library by calling `rpcExecute`.
    int rpc_ret = rpcExecute();
    (void)rpc_ret;

    // rpcExecute could fail, so you may want to have debug-printing here, and
    // then you should return.
    if(rpc_ret < 0) {
        debug("rpcExecute failed to execute with '%d'", ret);
    }

    return ret;
}
