#include "utils.cpp"
#include "index.cpp"
#include "distinct.cpp"
#include "condQuery.cpp"
#include "project.cpp"
#include "join.cpp"
#include "setOperations.cpp"


/**
 * @brief 输出操作结果表在本次读写中的相关信息
 * 
 * @param table 操作结果表的信息
 */
void print_IO_Info(table_t table) {
    if (table.start)
        printf("\n注：结果写入磁盘块：%d-%d\n", table.start, table.end);
    printf("本次共发生%ld次I/O\n\n", buff.numIO);
    system("pause");
}


/**
 * @brief 删除res表指向的文件
 * 
 * @param res 待删除的表的信息
 */
void dropResultTable(table_t res) {
    if (res.start && res.size)
        DropFiles(res.start);
    res.size = 0;
}


int main() {
    bufferInit();
    useCluster(table_R);
    useCluster(table_S);
    int select;
    table_t condQueryTable(condQueryStart);
    table_t projectTable(projStart);
    projectTable.rowSize = sizeOfRow / 2;
    table_t joinTable(joinResultStart);
    joinTable.rowSize = 2 * sizeOfRow;
    table_t setOperationTable(setOperationResultStart);

    while(1) {
        system("cls");
        printf("您想看哪个功能演示呢？\n\n");
        printf("====================================\n");
        printf("0. 退出\n");
        printf("1. 检索\n");
        printf("2. 投影\n");
        printf("3. 连接\n");
        printf("4. 集合操作\n");
        printf("====================================\n\n");
        printf("请给出您的选择：");
        cin >> select;

        if (select == 0) {
            printf("\n退出演示...\n\n");
            break;
        }
        switch(select) {
            case 0:
                break;
            case 1: {
                while (1) {
                    system("cls");
                    char tableName;
                    table_t table;
                    int val;
                    printf("您想看哪种检索方式的演示呢？\n\n");
                    printf("====================================\n");
                    printf("0. 回到上一级\n");
                    printf("1. 线性检索\n");
                    printf("2. 二分检索\n");
                    printf("3. 索引检索\n");
                    printf("====================================\n\n");
                    printf("请给出您的选择：");
                    cin >> select;

                    if (select == 0)
                        break;
                    else if (select < 0 || select > 3){
                        printf("不可以做出0-3以外的选择哦~\n");
                        system("pause");
                        continue;
                    }

                    system("cls");
                    if (select == 1)
                        printf("1. 线性检索\n");
                    else if (select == 2)
                        printf("2. 二分检索\n");
                    else if (select == 3)
                        printf("3. 索引检索\n");
                    
                    printf("\n您想看R表还是S表？(输入R或S)\n");
                    cin >> tableName;
                    if (tableName == 'R') {
                        table = table_R;
                        // val = 40;
                    } else if (tableName == 'S') {
                        table = table_S;
                        // val = 60;
                    } else {
                        printf("不可以输入R或S以外的任何东西噢~\n");
                        system("pause");
                        continue;
                    }
                    printf("请输入查找值：");
                    cin >> val;

                    dropResultTable(condQueryTable);
                    condQueryTable.start = condQueryStart;
                    clear_Buff_IO_Count();
                    if (select == 1) {
                        linearQuery(table, condQueryTable, val, EQ_cond);
                        showResult(condQueryTable);
                        print_IO_Info(condQueryTable);
                    } else if (select == 2) {
                        binaryQuery(table, condQueryTable, val, cmp);
                        showResult(condQueryTable);
                        print_IO_Info(condQueryTable);
                    } else if (select == 3) {
                        searchByIndex_and_Show(table, condQueryTable, val);
                        system("pause");
                    }
                };
                break;
            } case 2: {
                char tableName;
                system("cls");
                printf("由于功能限制，我们只能为您投影表的第一个属性\n\n");
                do {
                    printf("您想投影哪个表的第一个属性呢？(R或S)\n");
                    cin >> tableName;
                    dropResultTable(projectTable);
                    if (tableName == 'R') {
                        printf("将为您投影R的第一个属性：\n");
                        project(table_R, projectTable);
                        showResult(projectTable);
                        print_IO_Info(projectTable);
                    } else if (tableName == 'S') {
                        printf("将为您投影S的第一个属性：\n");
                        project(table_S, projectTable);
                        showResult(projectTable);
                        print_IO_Info(projectTable);
                    } else {
                        printf("不可以有R表或S表以外的选择哦~\n");
                        system("pause");
                    }
                    system("cls");
                } while (tableName != 'R' && tableName != 'S');
                break;
            } case 3: {
                while(1) {
                    system("cls");
                    printf("您想看哪种连接方式的演示呢？\n\n");
                    printf("====================================\n");
                    printf("0. 回到上一级\n");
                    printf("1. 嵌套循环连接(NEST-LOOP JOIN)\n");
                    printf("2. 排序归并连接(SORT-MERGE JOIN)\n");
                    printf("3. 散列连接(HASH JOIN)\n");
                    printf("====================================\n\n");
                    printf("请给出您的选择：");
                    cin >> select;

                    system("cls");
                    clear_Buff_IO_Count();
                    if (select == 0)
                        break;

                    dropResultTable(joinTable);
                    joinTable.start = joinResultStart;
                    if (select == 1) {
                        printf("查看嵌套循环连接(NEST-LOOP JOIN)的结果：\n");
                        joinTable = NEST_LOOP_JOIN(table_R, table_S);
                    } else if (select == 2) {
                        printf("查看排序归并连接(SORT-MERGE JOIN)的结果：\n");
                        joinTable = SORT_MERGE_JOIN(table_R, table_S);
                    } else if (select == 3) {
                        printf("查看散列连接(HASH JOIN)的结果：\n");
                        joinTable = HASH_JOIN(table_R, table_S);
                    } else {
                        printf("不可以做出0-3以外的选择哦~\n");
                        system("pause");
                        continue;
                    }
                    showResult(joinTable);
                    print_IO_Info(joinTable);
                }
                break;
            } case 4: {
                while(1) {
                    system("cls");
                    printf("您想看哪种集合操作的演示呢？\n\n");
                    printf("====================================\n");
                    printf("0. 回到上一级\n");
                    printf("1. 并\n");
                    printf("2. 交\n");
                    printf("3. 差\n");
                    printf("====================================\n\n");
                    printf("请给出您的选择：");
                    cin >> select;

                    system("cls");
                    clear_Buff_IO_Count();
                    if (select == 0)
                        break;

                    dropResultTable(setOperationTable);
                    setOperationTable.start = setOperationResultStart;
                    if (select == 1) {
                        printf("查看R∪S的结果：\n");
                        tablesUnion(table_R, table_S, setOperationTable);
                    } else if (select == 2) {
                        printf("查看R∩S的结果：\n");
                        tablesIntersect(table_R, table_S, setOperationTable);
                    } else if (select == 3) {
                        char diffedTable, diffTable;
                        table_t table1, table2;
                        printf("您想做哪个表对哪个表的差呢？(输入R S或S R)\n");
                        cin >> diffedTable >> diffTable;
                        if (diffedTable == 'R' && diffTable == 'S') {
                            printf("查看R-S的结果：\n");
                            table1 = table_R, table2 = table_S;
                        } else if (diffedTable == 'S' && diffTable == 'R') {
                            printf("查看S-R的结果：\n");
                            table1 = table_S, table2 = table_R;
                        } else {
                            printf("\"R S\"以及\"S R\"以外的输入都是不允许的哦~\n");
                            system("pause");
                            continue;
                        }
                        tablesDiff(table1, table2, setOperationTable);
                    } else {
                        printf("不可以做出0-3以外的选择哦~\n");
                        system("pause");
                        continue;
                    }
                    showResult(setOperationTable);
                    print_IO_Info(setOperationTable);
                }
                break;
            } default: {
                printf("不可以做出0-4以外的选择哦~\n");
                system("pause");
                break;
            }
        }
    }
    dropResultTable(condQueryTable);
    dropResultTable(projectTable);
    dropResultTable(joinTable);
    dropResultTable(setOperationTable);
    // 清除聚簇项
    for (auto iter = clusterTableMap.begin(); iter != clusterTableMap.end(); ++iter) {
        index_t addrItem = iter->second;
        addr_t clusterAddr = addrItem.A;
        DropFiles(clusterAddr);
    }
    // 清除索引项
    for (auto iter = indexTableMap.begin(); iter != indexTableMap.end(); ++iter) {
        index_t addrItem = iter->second;
        addr_t indexAddr = addrItem.A;
        DropFiles(indexAddr);
    }
    system("pause");
    return OK;
}