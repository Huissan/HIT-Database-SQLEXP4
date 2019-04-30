#include "utils.cpp"
#include "join.cpp"


int main() {
    /* Initialize the buffer */
    if (!initBuffer(520, 64, &globalBuf)) {
        perror("Buffer Initialization Failed!\n");
        return -1;
    }
    if (SORT_MERGE_JOIN() == FAIL) {
        system("pause");
        return FAIL;
    }

    system("pause");
    return OK;
}