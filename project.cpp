#include "utils.cpp"


/**
 * 投影操作：投影R.A
 */

const addr_t projStart = 2000;  // 投影结果的其实存放地址

/**
 * 投影
 * 从读取的表中抽取每一条记录的A属性写入结果
 * 
 * readStartAddr: 待投影表的起始地址
 * resStartAddr: 投影结果存放的起始地址
 * 
 * return: 投影结果存放区域的最后一块的地址
 */
addr_t project(addr_t readStartAddr, addr_t resStartAddr) {
    block_t blk, resBlk;
    row_t t_read, t_write;
    addr_t curAddr = 0;
    blk.loadFromDisk(readStartAddr);
    resBlk.writeInit(resStartAddr);
    bool isLastRow = false;
    while(1) {
        t_read = blk.getNewRow();
        if(t_read.isFilled == false) {
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            break;
        }
        t_write.A = t_read.A;
        t_read = blk.getNewRow();
        if(t_read.isFilled == false) {
            t_write.B = MAX_ATTR_VAL;
            isLastRow = true;
        } else {
            t_write.B = t_read.A;
        }
        curAddr = resBlk.writeRow(t_write);
    }
    return curAddr;
}


/**************************** main ****************************/
int main() {
    bufferInit();
    addr_t resEndAddr = project(R_start, projStart);
    showResult(projStart, sizeOfRow / 2);
    if (resEndAddr)
        printf("\n注：结果写入磁盘块：2000-%d\n", resEndAddr);
    printf("本次共发生%ld次I/O\n", buff.numIO);
    system("pause");
    return OK;
}