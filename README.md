# HIT-Database-SQLEXP4

哈工大数据库课程实验4  
C++版本


* 界面
    > main.cpp - 提供操作界面
* 数据
    > data/* - 包含本次实验用到的起始磁盘文件
* 工具
    > Block/* - 基于其中的extmem.h封装的迭代器以及相关的API  
    > BplusTree/* - B+树模板
    > utils.cpp - 提供一些诸如argmin、内排序等基本的轮子和基于Block类的操作
    > index.cpp - 索引相关API的实现  
    > distinct.cpp - 去重功能的实现  
* 任务要求功能
    > condQuery.cpp - 条件检索，包含线性检索、二分检索和索引检索  
    > project.cpp - 投影操作，基于属性A的投影
    > join.cpp - 连接操作，包含NEST-LOOP JOIN、SORT-MERGE JOIN和HASH JOIN
    > setOperations.cpp 集合操作，包含并、交、差  
    > recordGenerator.cpp - 随机生成R表和S表的记录  
* 其他
    > testBP.cpp - B+树的测试文件  
    > test_index.cpp - index.cpp的测试文件
