#include <ctime>
#include <cmath>
#include <numeric>
#include "Block.h"
#include "Block.cpp"

// A, B, C, D四个字段的取值范围
const int A_LOW = 1,  A_HIGH = 40;
const int B_LOW = 1,  B_HIGH = 1000;
const int C_LOW = 20, C_HIGH = 60;
const int D_LOW = 1,  D_HIGH = 1000;

// 
void generateANDwrite(int size, int start, int range[][2]) {
    Block blk;
    Row r;
    bool isLastRow = false;
    printf("Total write: %d\n", (int)ceil(size * 1.0 / numOfRowInBlk));
    try {
        blk.writeInit(start);
        for (int i = 0; i < size;) {
            // 生成7条记录，存入blk中
            for (int j = 0; j < numOfRowInBlk && i < size; ++i, ++j) {
                r.A = rand() % (range[0][1] - 1) + range[0][0];
                r.B = rand() % (range[1][1] - 1) + range[1][0];
                isLastRow = (j == numOfBufBlock - 1);
                blk.writeRow(r, isLastRow);
            }
        }
        if (blk.locateCursor() < addrOfLastRow)
            blk.freeBlock();
    } catch(std::bad_alloc e) {
        system("pause");
        exit(FAIL);
    }
}

int main() {
    int R_range[2][2] = {{A_LOW, A_HIGH}, {B_LOW, B_HIGH}};
    int S_range[2][2] = {{C_LOW, C_HIGH}, {D_LOW, D_HIGH}};
    generateANDwrite(R_size, R_start, R_range);
    generateANDwrite(S_size, S_start, S_range);
    return OK;
}
