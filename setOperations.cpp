#include "utils.cpp"


const addr_t setOperationResultStart = 4000;    // 集合操作结果的起始存放地址


/**
 * 表的并操作
 * 
 * return: 连接结果存放区域的最后一块的地址
 */
void tablesUnion(addr_t R_addr, addr_t S_addr) {
}

/**
 * 表的交操作
 * 采用嵌套循环的方法进行检索
 * 
 * R_addr: 第一个表在磁盘中的起始地址
 * S_addr: 第二个表在次盘中的起始地址
 * 
 * return: 连接结果存放区域的最后一块的地址
 */
addr_t tablesIntersect(addr_t R_addr, addr_t S_addr) {
    int numOfR = 6;
    int numOfRows_R = numOfRowInBlk * numOfR, numOfRows_S = numOfRowInBlk;
    addr_t curAddr = 0, resAddr = setOperationResultStart;
    
    block_t Rblk[numOfR], Sblk, resBlk;
    for (int i = 0; i < numOfR; ++i)
        Rblk[i].loadFromDisk(R_addr++);
    resBlk.writeInit(resAddr);
    row_t t_R[numOfRows_R], t_S[numOfRows_S];

    int readRows_R, readRows_S;
    while(1) {
        // 每次从R中读入6块
        readRows_R = read_N_Rows_From_M_Block(Rblk, t_R, numOfRows_R, numOfR);
        // printRows(t, readRows, val);
        Sblk.loadFromDisk(S_addr);
        while(1) {
            // 每次从S中读入1块进行比较
            readRows_S = read_N_Rows_From_1_Block(Sblk, t_S, numOfRows_S);
            for (int i = 0; i < numOfRows_R; ++i) {
                for (int j = 0; j < numOfRows_S; ++j) {
                    if (t_R[i] == t_S[j]) {
                        curAddr = resBlk.writeRow(t_R[i]);
                    }
                }
            }
            if (readRows_S < numOfRows_S)   // S读完了
                break;
        }
        if (readRows_R < numOfRows_R) {
            // R读完了
            addr_t endAddr = resBlk.writeLastBlock();
            if (endAddr != END_OF_FILE)
                curAddr = endAddr;
            break;
        }
    }
    return curAddr;
}

/**
 * 表的差操作
 * 
 * return: 连接结果存放区域的最后一块的地址
 */
void tablesDiff(addr_t R_addr, addr_t S_addr) {

}


/**************************** main ****************************/
int main() {
    bufferInit();
    // tablesUnion(R_start, S_start);
    addr_t resEndAddr = tablesIntersect(R_start, S_start);
    // tablesDiff(R_start, S_start);

    showResult(setOperationResultStart);
    if (resEndAddr)
        printf("\n注：结果写入起始磁盘块：4000-%d\n", resEndAddr);
    printf("本次共发生%ld次I/O\n\n", buff.numIO);

    system("pause");
    return OK;
}