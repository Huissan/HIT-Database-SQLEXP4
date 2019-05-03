#include "utils.cpp"
#pragma once


/**
 * @brief 投影操作
 * 投影R表中的非主码A属性
 */

const addr_t projStart = 2000;  // 投影结果的其实存放地址


/**
 * @brief 投影
 * 从读取的表中抽取每一条记录的A属性写入结果
 * 
 * @param projTable 待投影表的信息
 * @param resTable 投影结果表的信息
 */
void project(table_t projTable, table_t resTable) {
    block_t blk, resBlk;
    row_t t_read, t_write;
    addr_t curAddr = 0;
    blk.loadFromDisk(projTable.start);
    resBlk.writeInit(resTable.start);
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
        resBlk.writeRow(t_write);
        resTable.size += 1;
    }
    curAddr = resBlk.writeLastBlock();
    resTable.end = curAddr;
}


/**************************** main ****************************/
// int main() {
//     bufferInit();
//     table_t projRes(projStart);
//     project(table_R, projStart);
//     showResult(projStart, sizeOfRow / 2);
//     printf("\n注：结果写入磁盘块：%d-%d\n", projRes.start, projRes.end);
//     printf("本次共发生%ld次I/O\n", buff.numIO);
//     system("pause");
//     return OK;
// }