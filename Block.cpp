#include <iostream>
#include "Block.h"
extern "C" {
    #include "extmem.c"
}
#pragma once

#define CHAR_ZERO_ASCII '0'
#define CHAR_NINE_ASCII '9'
#define CHAR_EMPTY_ASCII 0

Block::Block() {
    // memset(blkData, 0, endOfBlock);
    endAddrOfData = addrOfLastRow;
}

/**
 * 块的写初始化
 * 
 * filename: 文件名，代表块地址
 * maxNumOfRows: 该块写入的最大记录数量
 */
void Block::writeInit(const file_t filename, int maxNumOfRows) {
    writeBlkAddr = filename;
    cursor = BLK_START_ADDR;
    if ((blkData = getNewBlockInBuffer(&buff)) == NULL){
        printf("Buffer Allocation Error!\n");
        system("pause");
        exit(FAIL);
    }
    memset(blkData, 0, endOfBlock);
    endAddrOfData = maxNumOfRows * sizeOfRow;
}

/**
 * 块的释放
 * 从缓冲区buff中释放该块
 */
void Block::freeBlock() {
    cursor = BLK_START_ADDR;        // 读写指针复位
    freeBlockInBuffer(blkData, &buff);
    memset(blkData, 0, endOfBlock); //　清空该块的数据
}

/**
 * 写入最后一块并释放写块
 */
addr_t Block::writeLastBlock() {
    if (cursor > BLK_START_ADDR) {
        // 表示写块里面还有剩余的内容
        writeToDisk();
        return writeBlkAddr - 1;
    } else {
        freeBlock();
        return 0;
    }
}

/**
 * 从当前的Block中获取一条新记录
 * 含当前块读完自动读取下一个地址到缓冲区的功能
 */
row_t Block::getNewRow() {
    row_t R;
    if (cursor == endAddrOfData) {
        // 该block的所有内容已读取完成，需要读取一块新的block进来
        addr_t nextAddr = readNextAddr();
        freeBlock();    // 想要继续申请必须先释放
        if (nextAddr == END_OF_FILE) {
            // 当前已读到文件末尾，即无下一块可读
            R.A = MAX_ATTR_VAL;
            R.B = MAX_ATTR_VAL;
            return R;
        } else {
            loadFromDisk(nextAddr, endAddrOfData);
        }
    }
    // 读取Row中的属性值A, B
    char str1[4] = "\0", str2[4] = "\0";
    for (int i = 0; i < 4; ++i) {
        str1[i] = *(blkData + cursor + i);
        if (str1[i] == '\0')
            str1[i] = 32;
        str2[i] = *(blkData + cursor + 4 + i);
        if (str2[i] == '\0')
            str2[i] = 32;
    }
    int scan1 = sscanf(str1, "%4d", &R.A);
    int scan2 = sscanf(str2, "%4d", &R.B);
    R.isFilled = true;
    if (scan1 <= 0)
        R.isFilled = false;
    if (scan2 <= 0)
        R.B = MAX_ATTR_VAL;
    cursor += sizeOfRow;    // cursor移动到下一条记录
    return R;
}

/**
 * 向当前的Block中写入一条新记录
 * 含缓冲区写满自动写入磁盘的功能
 */
addr_t Block::writeRow(const row_t R) {
    // 先判断block是否满
    if (cursor == endAddrOfData) {
        // printf("满一个块的结果，");
        try {
            addr_t next = writeBlkAddr + 1;
            _writeAddr(next);
            _writeToDisk(writeBlkAddr);
            if (next == END_OF_FILE) {
                writeBlkAddr = END_OF_FILE;
                cursor = endOfBlock;
            } else {
                // 写完后继续写，重新申请缓冲区
                if ((blkData = getNewBlockInBuffer(&buff)) == NULL)
                    throw std::bad_alloc();
                writeBlkAddr += 1;
                memset(blkData, 0, sizeOfRow * (numOfRowInBlk + 1));
                cursor = BLK_START_ADDR;
            }
        } catch(const std::bad_alloc &e) {
            std::cerr << e.what() << std::endl;
            printf("Buffer Allocation Error!\n");
            system("pause");
            exit(FAIL);
        }
    }
    // 再写数据到block上
    char a_buf[5] = "\0", b_buf[5] = "\0";
    if (R.A < MAX_ATTR_VAL)
        snprintf(a_buf, 5, "%-4d", R.A);
    if (R.B < MAX_ATTR_VAL)
        snprintf(b_buf, 5, "%-4d", R.B);
    for (int i = 0; i < 4; ++i) {
        if (a_buf[i] < CHAR_ZERO_ASCII || a_buf[i] > CHAR_NINE_ASCII)
            a_buf[i] = CHAR_EMPTY_ASCII;
        *(blkData + cursor + i) = a_buf[i];
        if (b_buf[i] < CHAR_ZERO_ASCII || b_buf[i] > CHAR_NINE_ASCII)
            b_buf[i] = CHAR_EMPTY_ASCII;
        *(blkData + cursor + 4 + i) = b_buf[i];
    }
    cursor += sizeOfRow;
    return writeBlkAddr;
}

/**
 * 读取当前块指向的下一个地址
 */
addr_t Block::readNextAddr() {
    char nextAddr[8] = "\0";
    unsigned int offset = addrOfLastRow;
    for (int i = 0; i < 8; ++i) {
        nextAddr[i] = *(blkData + offset + i);
    }
    addr_t next;
    if (sscanf(nextAddr, "%4d", &next) < 0)
        next = END_OF_FILE;
    return next;
}

/**
 * 从磁盘中加载地址为addr的磁盘块到缓冲区中
 * 
 * addr: 将要加载的磁盘块地址
 */
void Block::loadFromDisk(addr_t addr, cursor_t endPos) {
    if ((blkData = readBlockFromDisk(addr, &buff)) == NULL) {
        printf("Fail to read from disk %d!\n", addr);
        system("pause");
        exit(FAIL);
    }
    // printf("装载第%d块\n", addr);
    readBlkAddr = addr;
    cursor = BLK_START_ADDR;    // 指针复位
    endAddrOfData = endPos;
}

/**
 * 将缓冲区中的内容写入地址为nextAddr的磁盘块中
 * 
 * nextAddr: 将要写入的磁盘块地址
 */
void Block::writeToDisk(addr_t nextAddr) {
    _writeAddr(nextAddr);
    _writeToDisk(writeBlkAddr);
    writeBlkAddr += 1;
}

// -----------------------------------------------------------
//                       Private Methods                      
// -----------------------------------------------------------

void Block::_writeAddr(addr_t nextAddr) {
    char buf[9];
    unsigned int offset = addrOfLastRow;
    snprintf(buf, 8, "%-8d", nextAddr);
    for (int i = 0; i < 8; ++i) {
        if (buf[i] < CHAR_ZERO_ASCII || buf[i] > CHAR_NINE_ASCII)
            buf[i] = CHAR_EMPTY_ASCII;
        *(blkData + offset + i) = buf[i];
    }
}

void Block::_writeToDisk(addr_t addr) {
    if (writeBlockToDisk(blkData, addr, &buff) != 0) {
        printf("Fail to write to disk %d!\n", addr);
        system("pause");
        exit(FAIL);
    }
    // printf("写磁盘到第%d块\n", addr);
    cursor = BLK_START_ADDR;    // 指针复位
    memset(blkData, 0, endOfBlock);
}


// -----------------------------------------------------------
//                 Integrated Block Operations                
// -----------------------------------------------------------

/** 
 * 初始化全局缓冲区
 * 
 * return: 该记录的散列结果（所归属桶的编号）
 */
void bufferInit() {
    if (!initBuffer(520, 64, &buff)) {
        perror("Buffer Initialization Failed!\n");
        system("pause");
        exit(FAIL);
    }
}

void clear_Buff_IO_Count() { buff.numIO = 0; }

/**
 * 只利用1个Block，连续读取一个表中最多N条记录
 * 读取到表尾时，会自动归还缓冲区块
 * 前提：blk必须已被初始化(通过loadFromDisk等方法)
 * 
 * R: 记录数组
 * N: 记录数组的长度
 */
int read_N_Rows_From_1_Block(block_t &readBlk, row_t *R, int N) {
    int totalRead = N;
    for (int i = 0; i < N; ++i) {
        R[i] = readBlk.getNewRow();
        if (R[i].isFilled == false) {
            // 有些block不一定有完整的7条记录，一般是表的末尾
            totalRead = i;
            break;
        }
    }
    return totalRead;
}


/**
 * 利用M个Block，连续读取一个表中最多N条记录
 * 读取到表尾时，会自动归还缓冲区块
 * 前提：所有M个blk都必须已被初始化(通过loadFromDisk等方法)
 * 
 * R: 记录数组
 * N: 记录数组的最大长度
 * M: block的最大数量
 */
int read_N_Rows_From_M_Block(block_t *readBlk, row_t *R, int N, int M) {
    int totalRead = 0;
    row_t rtemp;
    // 从当前的M块中读取记录
    for (int i = 0; i < N; ) {
        for (int j = 0; j < M; ++j) {
            rtemp = readBlk[j].getNewRow();
            i += 1;
            if (rtemp.isFilled)
                R[totalRead++] = rtemp;
        }
    }
    addr_t next = readBlk[M - 1].readNextAddr();
    int count = 0;
    if (next == END_OF_FILE) {
        // printf("当前表的所有内容已装载完成！\n");
        next = readBlk[count].readNextAddr();
        while(next != END_OF_FILE) {
            next = readBlk[count].readNextAddr();
            readBlk[count].freeBlock();
            count += 1;
        }
    } else {
        while(count < M && next != END_OF_FILE) {
            // 装载后续的块
            readBlk[count].freeBlock();
            readBlk[count].loadFromDisk(next);
            next = readBlk[count].readNextAddr();
            count += 1;
        }
        for (int i = 0; i < M - count; ++i)
            // 归还没有用上的缓冲区块
            readBlk[count + i].freeBlock();
    }
    return totalRead;
}