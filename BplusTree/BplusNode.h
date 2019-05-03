#include <cstdio>
#include <cstdlib>
#include "../Block/Block.h"
#pragma once

#ifndef BPLUS_NODE
#define BPLUS_NODE

enum NODE_TYPE {INTERNAL, LEAF};				// 结点类型：内结点、叶子结点
enum SIBLING_DIRECTION {LEFT, RIGHT};			// 兄弟结点方向：左兄弟结点、右兄弟结点
typedef float key_t;                         	// 键类型
typedef addr_t tree_data_t;                    	// 值类型
extern const int ORDER = 8;                     // B+树的阶（非根内结点的最小子树个数）
extern const int MINNUM_KEY = ORDER - 1;		// 最小键值个数
extern const int MAXNUM_KEY = 2 * ORDER - 1;	// 最大键值个数
extern const int MINNUM_CHILD = MINNUM_KEY + 1; // 最小子树个数
extern const int MAXNUM_CHILD = MAXNUM_KEY + 1; // 最大子树个数
extern const int MINNUM_LEAF = MINNUM_KEY;      // 最小叶子结点键值个数
extern const int MAXNUM_LEAF = MAXNUM_KEY;      // 最大叶子结点键值个数

extern const tree_data_t INVALID_INDEX = (addr_t)-1;

// 结点基类
class BplusNode{
public:
	BplusNode() { setKeyNum(0); }
	virtual ~BplusNode() { setKeyNum(0); }
 
	NODE_TYPE getType() const { return m_Type; }
	void setType(NODE_TYPE type){ m_Type = type; }
	int getKeyNum() const { return m_KeyNum; }
	void setKeyNum(int n) { m_KeyNum = n; }
	key_t getKeyValue(int i) const { return m_KeyValues[i]; }
	void setKeyValue(int i, key_t key) { m_KeyValues[i] = key; }
	int getKeyIndex(key_t key) const;  
    
    // 清空结点，同时会清空结点所包含的子树结点
	virtual void clear() = 0; 
    // 从结点中移除键值
	virtual void removeKey(int keyIndex, int childIndex) = 0;
    // 分裂结点
	virtual void split(BplusNode *parentNode, int childIndex) = 0;
    // 合并结点
	virtual void mergeChild(BplusNode *parentNode, BplusNode *childNode, int keyIndex) = 0;
    // 从兄弟结点中借一个键值
	virtual void borrowFrom(BplusNode *destNode, BplusNode *parentNode, int keyIndex, SIBLING_DIRECTION d) = 0;
    // 根据键值获取孩子结点指针下标
	virtual int getChildIndex(key_t key, int keyIndex) const = 0; 
protected:
	NODE_TYPE m_Type;	// 节点类型
	int m_KeyNum;		// 当前节点的孩子节点数
	key_t m_KeyValues[MAXNUM_KEY];	// 节点值
};

/**
 * @brief 折半查找查询键值key在结点中存储的下标
 * 
 * @param key 查询键值
 * @return int 键值对应的下标
 */
int BplusNode::getKeyIndex(key_t key) const {
	int left = 0;
    int right = getKeyNum();
    int current;
	do {
        current = (left + right) / 2;
        key_t currentKey = getKeyValue(current);
		if (key > currentKey)
            left = current + 1;
        else
			right = current;
	} while (left < right);
	return left;
}

#endif
