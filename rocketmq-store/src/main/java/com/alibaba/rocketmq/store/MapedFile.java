/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.store;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.config.FlushDiskType;
import com.alibaba.rocketmq.store.util.LibC;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author shijia.wxr
 */
public class MapedFile extends ReferenceResource {
    public static final int OS_PAGE_SIZE = 1024 * 4;
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    /**
     * 总的映射的内存数
     */
    private static final AtomicLong TotalMapedVitualMemory = new AtomicLong(0);

    /**
     * 映射的文件数目
     */
    private static final AtomicInteger TotalMapedFiles = new AtomicInteger(0);

    /**
     * 文件名
     */
    private final String fileName;

    /**
     * 文件偏移量从文件名上获取
     */
    private final long fileFromOffset;

    /**
     * 文件大小
     */
    private final int fileSize;

    private final File file;

    /**
     * 文件内存映射句柄
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 写的位置偏移量
     */
    private final AtomicInteger wrotePostion = new AtomicInteger(0);

    /**
     * 提交的位置偏移量
     */
    private final AtomicInteger committedPosition = new AtomicInteger(0);

    private FileChannel fileChannel;

    /**
     * 保存时间戳
     */
    private volatile long storeTimestamp = 0;
    
    /**
     * 
     */
    private boolean firstCreateInQueue = false;

    /**
     * 映射文件
     * @param fileName 文件名
     * @param fileSize 文件大小
     * @throws IOException
     */
    public MapedFile(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TotalMapedVitualMemory.addAndGet(fileSize);
            TotalMapedFiles.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }


    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 清理内存映射资源
     * 参考http://bugs.java.com/bugdatabase/view_bug.do?bug_id=6893654
     * 通过调用cleaner方法彻底清理内存资源
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
        
        //
        ByteBuffer buf = viewed(buffer);
        Object obj = invoke(buf, "cleaner");
        invoke(obj, "clean");
    }

    /**
     * @描述: 调用目标对象的指定方法和传递参数 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：     
     * @return Object   
     * @throws
     */
    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
    	/**
    	 * jvm安全控制管理，启用特权，执行指定的 PrivilegedAction。该操作在调用者保护域所拥有的全部 权限下执行
    	 */
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                	//获取指定的方法
                    Method method = method(target, methodName, args);
                    //设置方法属性为可见
                    method.setAccessible(true);
                    //调用该方法
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    /**
     * @描述:查找目标对象的方法 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：     
     * @return Method   
     * @throws
     */
    private static Method method(Object target, String methodName, Class<?>[] args)
            throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }


    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        //获取类的所有方法
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
        	//判断方法名是否和"attachment"相等
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        /**
         * 
         */
        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }


    public static int getTotalmapedfiles() {
        return TotalMapedFiles.get();
    }


    public static long getTotalMapedVitualMemory() {
        return TotalMapedVitualMemory.get();
    }


    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * @描述: 向内存映射文件追加消息数据 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月21日
     * @修改内容
     * @参数：	msg  消息对象
     * 			cb   回调对象
     * @return AppendMessageResult   
     * @throws
     */
    public AppendMessageResult appendMessage(final Object msg, final AppendMessageCallback cb) {
        assert msg != null;
        assert cb != null;

        /**
         * 获取当前写的位置
         */
        int currentPos = this.wrotePostion.get();

        /**
         * 判断写的位置是否超出文件大小
         */
        if (currentPos < this.fileSize) 
        {
        	//获取内存映射缓冲区,位置为0
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, msg);
            this.wrotePostion.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }

        log.error("MapedFile.appendMessage return null, wrotePostion: " + currentPos + " fileSize: " + this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * @描述: 获取文件偏移量 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月21日
     * @修改内容
     * @参数：     
     * @return long   
     * @throws
     */
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    /**
     * @描述: 追加数据 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月21日
     * @修改内容
     * @参数：     
     * @return boolean   
     * @throws
     */
    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePostion.get();
        if ((currentPos + data.length) <= this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            byteBuffer.put(data);
            this.wrotePostion.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**

     *
     * @param flushLeastPages

     *
     * @return
     */
    public int commit(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = this.wrotePostion.get();
                this.mappedByteBuffer.force();
                this.committedPosition.set(value);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
                this.committedPosition.set(this.wrotePostion.get());
            }
        }

        return this.getCommittedPosition();
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePostion.get();


        if (this.isFull()) {
            return true;
        }


        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    public int getCommittedPosition() {
        return committedPosition.get();
    }


    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePostion.get();
    }

    public SelectMapedBufferResult selectMapedBuffer(int pos, int size) {

        if ((pos + size) <= this.wrotePostion.get()) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        }

        else {
            log.warn("selectMapedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }


        return null;
    }

    /**

     */
    public SelectMapedBufferResult selectMapedBuffer(int pos) {
        if (pos < this.wrotePostion.get() && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = this.wrotePostion.get() - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMapedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }


        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmaping.");
            return false;
        }

        /**
         * 是否清理完成
         */
        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TotalMapedVitualMemory.addAndGet(this.fileSize * (-1));
        TotalMapedFiles.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    /**
     * @描述: 销毁映射文件 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@param intervalForcibly
     * @参数：@return     
     * @return boolean   
     * @throws
     */
    public boolean destroy(final long intervalForcibly) 
    {
        //去除引用计数
    	this.shutdown(intervalForcibly);

    	/**
    	 * 是否清理完成
    	 */
        if (this.isCleanupOver()) {
            try {
            	//关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                //删除文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePostion() + " M:"
                        + this.getCommittedPosition() + ", "
                        + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy maped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePostion() {
        return wrotePostion.get();
    }

    public void setWrotePostion(int pos) {
        this.wrotePostion.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MapedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file worm up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file worm up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }


    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }


    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
