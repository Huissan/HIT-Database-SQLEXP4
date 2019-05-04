#include <ctime>
#include <cmath>
#include <numeric>
#include <vector>
#include "utils.cpp"


// A, B, C, D四个字段的取值范围
const int A_LOW = 1,  A_HIGH = 40;
const int B_LOW = 1,  B_HIGH = 1000;
const int C_LOW = 20, C_HIGH = 60;
const int D_LOW = 1,  D_HIGH = 1000;
const int maxOverlaps = 100;
const int threshold = 15;
std::vector<row_t> overlaps;


/**
 * @brief 随机生成一个表的数据写入到磁盘中
 * 
 * @param table 表的信息，包括起始地址、结束地址以及记录数量和长度等
 * @param range 每个属性的生成范围取值
 */
void generateANDwrite(table_t table, int range[][2]) {
    Block blk;
    blk.writeInit(table.start);
    row_t r;
    addr_t curAddr = 0;
    for (int i = 0, count = 0; i < table.size;) {
        // 生成7条记录，存入blk中
        for (int j = 0; j < numOfRowInBlk && i < table.size; ++i, ++j) {
            int prob = rand() % 101;
            if (prob < threshold && count <= maxOverlaps) {
                r = overlaps[count++];
            } else {
                r.A = rand() % (range[0][1] - range[0][0]) + range[0][0];
                r.B = rand() % (range[1][1] - range[1][0]) + range[1][0];
            }
            curAddr = blk.writeRow(r);
            if (i == table.size - 1) {
                if (count == 101)
                    count -= 1;
                printf("使用了重复记录集中记录的数量：%d\n", count);
            }
        }
    }
    curAddr = blk.writeLastBlock();
    table.end = curAddr;
    printf("写入完成！结果写入磁盘块：%d-%d\n\n", table.start, table.end);
}

/**************************** main ****************************/
int main() {
    bufferInit();
    overlaps.resize(maxOverlaps);
    // 生成重复记录集
    row_t t;
    srand(time(NULL));
    for (int i = 0; i < maxOverlaps; ++i) {
        t.A = rand() % (A_HIGH - C_LOW) + C_LOW;
        t.B = rand() % (B_HIGH - B_LOW) + B_LOW;
        overlaps.push_back(t);
    }
    // 生成不同表的记录
    srand(time(NULL));
    int R_range[2][2] = {{A_LOW, A_HIGH}, {B_LOW, B_HIGH}};
    int S_range[2][2] = {{C_LOW, C_HIGH}, {D_LOW, D_HIGH}};
    printf("\n=================== 生成R表的记录 ===================\n");
    generateANDwrite(table_R, R_range);
    printf("\n=================== 生成S表的记录 ===================\n");
    generateANDwrite(table_S, S_range);
    system("pause");
    return OK;
}
