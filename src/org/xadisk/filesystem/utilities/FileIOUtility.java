/*
Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

This source code is being made available to the public under the terms specified in the license
"Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
 */


package org.xadisk.filesystem.utilities;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import org.xadisk.filesystem.NativeXAFileSystem;

public class FileIOUtility {

    public static void renameTo(File src, File dest) throws IOException {
        if (!src.renameTo(dest)) {
            if (renamePossible(src, dest)) {
                while (!src.renameTo(dest)) {
					makeSpaceForGC();
                    if (src.renameTo(dest)) {
                        break;
                    }
                    if (!src.isDirectory()) {
                        FileChannel srcFC = new FileInputStream(src).getChannel();
                        FileChannel destFC = new FileOutputStream(dest).getChannel();
                        long size = srcFC.size();
                        long num = 0;
                        while (num < size) {
                            num += destFC.transferFrom(srcFC, num, NativeXAFileSystem.maxTransferToChannel(size - num));
                        }
                        srcFC.close();
                        destFC.close();
						dest.setLastModified(src.lastModified());
                        deleteFile(src);
                    }
                }
            } else {
                throw new IOException("Rename not feasible from " + src + " to " + dest);
            }
        }
    }

    private static boolean renamePossible(File src, File dest) {
        return src.exists() && !dest.exists() && src.getParentFile().canWrite() && dest.getParentFile().canWrite();
    }

    public static void deleteFile(File f) throws IOException {
        if (f.delete()) {
            return;
        }
        if (!f.getParentFile().canWrite()) {
            throw new IOException("Parent directory not writable.");
        }
        if (!f.exists()) {
            throw new IOException("File does not exist.");
        }
        while (!f.delete()) {
			makeSpaceForGC();
        }
    }

    private static void deleteEmptyDirectory(File dir) throws IOException {
        deleteFile(dir);
    }

    public static void deleteDirectoryRecursively(File dir) throws IOException {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            if (files[i].isDirectory()) {
                deleteDirectoryRecursively(files[i]);
            } else {
                deleteFile(files[i]);
            }
        }
        deleteEmptyDirectory(dir);
    }

    public static void createFile(File f) throws IOException {
        if (f.createNewFile()) {
            return;
        }
        if (f.exists()) {
            throw new IOException("File already exists.");
        }
        if (!f.getParentFile().canWrite()) {
            throw new IOException("Parent directory not writable.");
        }
        while (!f.createNewFile()) {
			makeSpaceForGC();
        }
    }

    public static void createDirectory(File dir) throws IOException {
        if (dir.mkdir()) {
            return;
        }
        if (dir.exists()) {
            throw new IOException("File already exists.");
        }
        if (!dir.getParentFile().canWrite()) {
            throw new IOException("Parent directory not writable.");
        }
        while (!dir.mkdir()) {
			makeSpaceForGC();
        }
    }

    public static void createDirectoriesIfRequired(File dir) throws IOException {
        if (dir.isDirectory()) {
            return;
        }
        createDirectoriesIfRequired(dir.getParentFile());
        createDirectory(dir);
    }

    public static String[] listDirectoryContents(File dir) throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException("The directory doesn't exist.");
        }
        if (!dir.canRead()) {
            throw new IOException("The directory is not readable.");
        }
        String children[] = dir.list();
        while (children == null) {
			makeSpaceForGC();
            children = dir.list();
        }
        return children;
    }

    private static void makeSpaceForGC() throws IOException {
        /**
         * I know that this mechanism of doing gc when file operations fail is weird. But I had no other option than to use
         * this workaround which would get triggered very very rarely (when that jvm bug gets triggered). Things like
         * not closing channel/stream are ususally the cause for file delete/rename failure, but a check over the
         * complete xadisk code confirms that the cause here is something else (a jvm bug).
        **/
        System.gc();
        System.gc();
        System.gc();
		try {
			Thread.sleep(100);
		} catch(InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw (IOException) new IOException().initCause(ie);
		}
    }

    public static void readFromChannel(FileChannel fc, ByteBuffer buffer, int bufferOffset, int num)
            throws IOException, EOFException {
        buffer.position(bufferOffset);
        if (buffer.remaining() < num) {
            throw new BufferUnderflowException();
        }
        buffer.limit(bufferOffset + num);
        int numRead = 0;
        int t = 0;
        while (numRead < num) {
            t = fc.read(buffer);
            if (t == -1) {
                throw new EOFException();
            }
            numRead += t;
        }
    }

    public static void copyFile(File src, File dest, boolean append) throws IOException {
        FileChannel srcChannel = new FileInputStream(src).getChannel();
        FileChannel destChannel = new FileOutputStream(dest, append).getChannel();
        long contentLength = srcChannel.size();
        long num = 0;
        while (num < contentLength) {
            num += srcChannel.transferTo(num, NativeXAFileSystem.maxTransferToChannel(contentLength - num), destChannel);
        }

        destChannel.force(false);
        srcChannel.close();
        destChannel.close();
    }

    public static void copyFile(InputStream srcStream, File dest, boolean append) throws IOException {
        FileOutputStream destStream = new FileOutputStream(dest, append);
        byte[] b = new byte[1000];
        int numRead = 0;
        while (true) {
            numRead = srcStream.read(b);
            if (numRead == -1) {
                break;
            }
            destStream.write(b, 0, numRead);
        }

        destStream.flush();
        destStream.close();
        srcStream.close();
    }
}
