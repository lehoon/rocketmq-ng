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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * 内存映射文件队列
 * @author shijia.wxr
 */
public class MapedFileQueue {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private static final Logger logError = LoggerFactory.getLogger(LoggerName.StoreErrorLoggerName);

    /**
     * 批量删除文件最大值默认为10
     */
    private static final int DeleteFilesBatchMax = 10;

    /**
     * 文件存储路径
     */
    private final String storePath;

    /**
     * 内存文件映射大小
     */
    private final int mapedFileSize;

    /**
     * 映射文件队列
     */
    private final List<MapedFile> mapedFiles = new ArrayList<MapedFile>();
    
    /**
     * 读写锁
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final AllocateMapedFileService allocateMapedFileService;

    /**
     * 最后提交位置
     */
    private long committedWhere = 0;

    private volatile long storeTimestamp = 0;

    /**
     * 构造函数
     * @param storePath  存储路径
     * @param mapedFileSize  映射文件大小
     * @param allocateMapedFileService 分配文件内存映射服务
     */
    public MapedFileQueue(final String storePath, int mapedFileSize, AllocateMapedFileService allocateMapedFileService) 
    {
        this.storePath = storePath;
        this.mapedFileSize = mapedFileSize;
        this.allocateMapedFileService = allocateMapedFileService;
    }


    /**
     * @描述: 自检 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：     
     * @return void   
     * @throws
     */
    public void checkSelf() {
        this.readWriteLock.readLock().lock();
        try {
            if (!this.mapedFiles.isEmpty()) {
                MapedFile first = this.mapedFiles.get(0);
                MapedFile last = this.mapedFiles.get(this.mapedFiles.size() - 1);

                //实际个数
                int sizeCompute = (int) ((last.getFileFromOffset() - first.getFileFromOffset()) / this.mapedFileSize) + 1;
                //队列大小，也就是内存映射文件个数  1g一个文件
                int sizeReal = this.mapedFiles.size();
                
                if (sizeCompute != sizeReal) 
                {
                    logError
                            .error(
                                    "[BUG]The mapedfile queue's data is damaged, {} mapedFileSize={} sizeCompute={} sizeReal={}\n{}", //
                                    this.storePath,//
                                    this.mapedFileSize,//
                                    sizeCompute,//
                                    sizeReal,//
                                    this.mapedFiles.toString()//
                            );
                }
            }
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }

    /**
     * @描述:根据时间戳查询映射文件 ,按照文件最后修改时间匹配
     * 否则返回最后一个文件
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@param timestamp 时间戳
     * @参数：@return     
     * @return MapedFile   
     * @throws
     */
    public MapedFile getMapedFileByTime(final long timestamp) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MapedFile mapedFile = (MapedFile) mfs[i];
            if (mapedFile.getLastModifiedTimestamp() >= timestamp) {
                return mapedFile;
            }
        }

        return (MapedFile) mfs[mfs.length - 1];
    }

    /**
     * @描述: 返回映射对象数组 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@param reservedMapedFiles
     * @参数：@return     
     * @return Object[]   
     * @throws
     */
    private Object[] copyMapedFiles(final int reservedMapedFiles) {
        Object[] mfs = null;

        try {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.size() <= reservedMapedFiles) {
                return null;
            }

            mfs = this.mapedFiles.toArray();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
        return mfs;
    }

    /**
     * @描述: 删除脏数据文件 ，删除指定偏移量之后的文件
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@param offset 偏移量     
     * @return void   
     * @throws
     */
    public void truncateDirtyFiles(long offset) {
        List<MapedFile> willRemoveFiles = new ArrayList<MapedFile>();

        for (MapedFile file : this.mapedFiles) {
        	/**
        	 * 计算该映射文件的最大偏移量
        	 * 开始偏移量 + 文件映射大小 = 映射文件最大偏移量
        	 */
            long fileTailOffset = file.getFileFromOffset() + this.mapedFileSize;
            if (fileTailOffset > offset) {
            	//如果指定偏移量大于该文件的开始偏移量，则需要重新设置该文件的有效偏移量
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePostion((int) (offset % this.mapedFileSize));
                    file.setCommittedPosition((int) (offset % this.mapedFileSize));
                } else {
                	//如果指定的偏移量小于该文件的开始偏移量，则表明需要删除该文件，不需要重新设定写、提交的偏移量即可
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        //删除映射队列里面的引用
        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * @描述: 从有效映射文件队列里面移除指定的映射文件 
     * 真实的映射对象已经安全的销毁，所以只移除引用即可
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@param files      待删除的文件列表
     * @return void   
     * @throws
     */
    private void deleteExpiredFile(List<MapedFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (MapedFile file : files) {
                    if (!this.mapedFiles.remove(file)) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }

    /**
     * @描述: 加载映射文件 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@return     
     * @return boolean   
     * @throws
     */
    public boolean load() {
    	//初始化映射文件路径
        File dir = new File(this.storePath);
        //获取文件列表
        File[] files = dir.listFiles();
        
        if (files != null) {
            //升序排列文件数组，根据对象的compareTo方法比较
            Arrays.sort(files);
            for (File file : files) {

            	//如果文件长度不等于指定的文件大小，需要报错返回
                if (file.length() != this.mapedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, ignore it");
                    return true;
                }


                /**
                 * 创建映射文件队列
                 */
                try {
                    MapedFile mapedFile = new MapedFile(file.getPath(), mapedFileSize);

                    mapedFile.setWrotePostion(this.mapedFileSize);
                    mapedFile.setCommittedPosition(this.mapedFileSize);
                    this.mapedFiles.add(mapedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }


    /**
     * @描述: 计算还有多少可用的文件 
     * @作者: zgzhang@txbds.com
     * @日期:2016年11月22日
     * @修改内容
     * @参数：@return     
     * @return long   
     * @throws
     */
    public long howMuchFallBehind() {
        if (this.mapedFiles.isEmpty())
            return 0;

        long committed = this.committedWhere;
        if (committed != 0) {
            MapedFile mapedFile = this.getLastMapedFile(0, false);
            if (mapedFile != null) {
                return (mapedFile.getFileFromOffset() + mapedFile.getWrotePostion()) - committed;
            }
        }

        return 0;
    }


    public MapedFile getLastMapedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MapedFile mapedFileLast = null;
        {
            this.readWriteLock.readLock().lock();
            if (this.mapedFiles.isEmpty()) {
                createOffset = startOffset - (startOffset % this.mapedFileSize);
            } else {
                mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
            }
            this.readWriteLock.readLock().unlock();
        }

        if (mapedFileLast != null && mapedFileLast.isFull()) {
            createOffset = mapedFileLast.getFileFromOffset() + this.mapedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath =
                    this.storePath + File.separator
                            + UtilAll.offset2FileName(createOffset + this.mapedFileSize);
            MapedFile mapedFile = null;

            if (this.allocateMapedFileService != null) {
                mapedFile =
                        this.allocateMapedFileService.putRequestAndReturnMapedFile(nextFilePath,
                                nextNextFilePath, this.mapedFileSize);
            } else {
                try {
                    mapedFile = new MapedFile(nextFilePath, this.mapedFileSize);
                } catch (IOException e) {
                    log.error("create mapedfile exception", e);
                }
            }

            if (mapedFile != null) {
                this.readWriteLock.writeLock().lock();
                if (this.mapedFiles.isEmpty()) {
                    mapedFile.setFirstCreateInQueue(true);
                }
                this.mapedFiles.add(mapedFile);
                this.readWriteLock.writeLock().unlock();
            }

            return mapedFile;
        }

        return mapedFileLast;
    }

    public MapedFile getLastMapedFile() {
        return this.getLastMapedFile(0);
    }

    public MapedFile getLastMapedFile(final long startOffset) {
        return getLastMapedFile(startOffset, true);
    }

    public MapedFile getLastMapedFileWithLock() {
        MapedFile mapedFileLast = null;
        this.readWriteLock.readLock().lock();
        if (!this.mapedFiles.isEmpty()) {
            mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
        }
        this.readWriteLock.readLock().unlock();

        return mapedFileLast;
    }

    public boolean resetOffset(long offset) {
        this.readWriteLock.writeLock().lock();
        if (!this.mapedFiles.isEmpty()) {
            MapedFile mapedFileLast = this.mapedFiles.get(this.mapedFiles.size() - 1);
            long lastOffset = mapedFileLast.getFileFromOffset() +
                    mapedFileLast.getWrotePostion();
            long diff = lastOffset - offset;

            final int maxdiff = 1024 * 1024 * 1024 * 2;

            if (diff > maxdiff) return false;
        }

        for (int i = this.mapedFiles.size() - 1; i >= 0; i--) {
            MapedFile mapedFileLast = this.mapedFiles.get(i);

            if (offset >= mapedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mapedFileLast.getFileSize());
                mapedFileLast.setCommittedPosition(where);
                mapedFileLast.setWrotePostion(where);
                break;
            } else {
                this.mapedFiles.remove(mapedFileLast);
            }
        }
        this.readWriteLock.writeLock().unlock();
        return true;
    }

    public long getMinOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                return this.mapedFiles.get(0).getFileFromOffset();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return -1;
    }


    public long getMaxOffset() {
        try {
            this.readWriteLock.readLock().lock();
            if (!this.mapedFiles.isEmpty()) {
                int lastIndex = this.mapedFiles.size() - 1;
                MapedFile mapedFile = this.mapedFiles.get(lastIndex);
                return mapedFile.getFileFromOffset() + mapedFile.getWrotePostion();
            }
        } catch (Exception e) {
            log.error("getMinOffset has exception.", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return 0;
    }

    public void deleteLastMapedFile() {
        if (!this.mapedFiles.isEmpty()) {
            int lastIndex = this.mapedFiles.size() - 1;
            MapedFile mapedFile = this.mapedFiles.get(lastIndex);
            mapedFile.destroy(1000);
            this.mapedFiles.remove(mapedFile);
            log.info("on recover, destroy a logic maped file " + mapedFile.getFileName());
        }
    }

    public int deleteExpiredFileByTime(//
                                       final long expiredTime, //
                                       final int deleteFilesInterval, //
                                       final long intervalForcibly,//
                                       final boolean cleanImmediately//
    ) {
        Object[] mfs = this.copyMapedFiles(0);

        if (null == mfs)
            return 0;


        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MapedFile> files = new ArrayList<MapedFile>();
        if (null != mfs) {
            for (int i = 0; i < mfsLength; i++) {
                MapedFile mapedFile = (MapedFile) mfs[i];
                long liveMaxTimestamp = mapedFile.getLastModifiedTimestamp() + expiredTime;
                if (System.currentTimeMillis() >= liveMaxTimestamp//
                        || cleanImmediately) {
                    if (mapedFile.destroy(intervalForcibly)) {
                        files.add(mapedFile);
                        deleteCount++;

                        if (files.size() >= DeleteFilesBatchMax) {
                            break;
                        }

                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMapedFiles(0);

        List<MapedFile> files = new ArrayList<MapedFile>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = true;
                MapedFile mapedFile = (MapedFile) mfs[i];
                SelectMapedBufferResult result = mapedFile.selectMapedBuffer(this.mapedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();

                    destroy = (maxOffsetInLogicQueue < offset);
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mapedfile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else {
                    log.warn("this being not excuted forever.");
                    break;
                }

                if (destroy && mapedFile.destroy(1000 * 60)) {
                    files.add(mapedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }


    public boolean commit(final int flushLeastPages) {
        boolean result = true;
        MapedFile mapedFile = this.findMapedFileByOffset(this.committedWhere, true);
        if (mapedFile != null) {
            long tmpTimeStamp = mapedFile.getStoreTimestamp();
            int offset = mapedFile.commit(flushLeastPages);
            long where = mapedFile.getFileFromOffset() + offset;
            result = (where == this.committedWhere);
            this.committedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }


    public MapedFile findMapedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            this.readWriteLock.readLock().lock();
            MapedFile mapedFile = this.getFirstMapedFile();

            if (mapedFile != null) {
                int index =
                        (int) ((offset / this.mapedFileSize) - (mapedFile.getFileFromOffset() / this.mapedFileSize));
                if (index < 0 || index >= this.mapedFiles.size()) {
                    logError
                            .warn(
                                    "findMapedFileByOffset offset not matched, request Offset: {}, index: {}, mapedFileSize: {}, mapedFiles count: {}, StackTrace: {}",//
                                    offset,//
                                    index,//
                                    this.mapedFileSize,//
                                    this.mapedFiles.size(),//
                                    UtilAll.currentStackTrace());
                }

                try {
                    return this.mapedFiles.get(index);
                } catch (Exception e) {
                    if (returnFirstOnNotFound) {
                        return mapedFile;
                    }
                }
            }
        } catch (Exception e) {
            log.error("findMapedFileByOffset Exception", e);
        } finally {
            this.readWriteLock.readLock().unlock();
        }

        return null;
    }


    private MapedFile getFirstMapedFile() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }

        return this.mapedFiles.get(0);
    }


    public MapedFile getLastMapedFile2() {
        if (this.mapedFiles.isEmpty()) {
            return null;
        }
        return this.mapedFiles.get(this.mapedFiles.size() - 1);
    }


    public MapedFile findMapedFileByOffset(final long offset) {
        return findMapedFileByOffset(offset, false);
    }


    public long getMapedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMapedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mapedFileSize;
                }
            }
        }

        return size;
    }


    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MapedFile mapedFile = this.getFirstMapedFileOnLock();
        if (mapedFile != null) {
            if (!mapedFile.isAvailable()) {
                log.warn("the mapedfile was destroyed once, but still alive, " + mapedFile.getFileName());
                boolean result = mapedFile.destroy(intervalForcibly);
                if (result) {
                    log.warn("the mapedfile redelete OK, " + mapedFile.getFileName());
                    List<MapedFile> tmps = new ArrayList<MapedFile>();
                    tmps.add(mapedFile);
                    this.deleteExpiredFile(tmps);
                } else {
                    log.warn("the mapedfile redelete Failed, " + mapedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }


    public MapedFile getFirstMapedFileOnLock() {
        try {
            this.readWriteLock.readLock().lock();
            return this.getFirstMapedFile();
        } finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public void shutdown(final long intervalForcibly) {
        this.readWriteLock.readLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            mf.shutdown(intervalForcibly);
        }
        this.readWriteLock.readLock().unlock();
    }


    public void destroy() {
        this.readWriteLock.writeLock().lock();
        for (MapedFile mf : this.mapedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mapedFiles.clear();
        this.committedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
        this.readWriteLock.writeLock().unlock();
    }


    public long getCommittedWhere() {
        return committedWhere;
    }


    public void setCommittedWhere(long committedWhere) {
        this.committedWhere = committedWhere;
    }


    public long getStoreTimestamp() {
        return storeTimestamp;
    }


    public List<MapedFile> getMapedFiles() {
        return mapedFiles;
    }


    public int getMapedFileSize() {
        return mapedFileSize;
    }
}
