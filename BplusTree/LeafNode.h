#include "BplusNode.h"
#include "InternalNode.h"
#pragma once

#ifndef LEAF_NODE
#define LEAF_NODE

class LeafNode : public BplusNode{
public:
	LeafNode():BplusNode() {
        setType(LEAF);
        setLeftSibling(NULL); setRightSibling(NULL);
    }
    ~LeafNode() {}

    LeafNode *getLeftSibling() const { return m_LeftSibling; }
	void setLeftSibling(LeafNode *node) { m_LeftSibling = node; }
	LeafNode *getRightSibling() const { return m_RightSibling; }
	void setRightSibling(LeafNode *node) { m_RightSibling = node; }
	tree_data_t getData(int i) const { return m_Datas[i]; }
	void setData(int i, const tree_data_t &data) { m_Datas[i] = data; }
	void insert(key_t key, const tree_data_t &data);
    
	virtual void split(BplusNode *parentNode, int childIndex);
	virtual void mergeChild(BplusNode *parentNode, BplusNode *childNode, int keyIndex);
	virtual void removeKey(int keyIndex, int childIndex);
	virtual void clear();
	virtual void borrowFrom(BplusNode *destNode, BplusNode *parentNode, int keyIndex, SIBLING_DIRECTION d);
	virtual int getChildIndex(key_t key, int keyIndex)const;
private:
	LeafNode *m_LeftSibling;
	LeafNode *m_RightSibling;
	tree_data_t m_Datas[MAXNUM_LEAF];
};

#endif // !LEAF_NODE

/**
 * @brief 插入一个叶节点
 * 
 * @param key 待插入叶结点的键值
 * @param data 待插入叶节点的数值
 */
void LeafNode::insert(key_t key, const tree_data_t &data) {
	int i;
	for (i = m_KeyNum; i >= 1 && m_KeyValues[i - 1] > key; --i) {
		setKeyValue(i, m_KeyValues[i - 1]);
		setData(i, m_Datas[i - 1]);
	}
	setKeyValue(i, key);
	setData(i, data);
	setKeyNum(m_KeyNum + 1);
}

/**
 * @brief 清空所有叶节点
 */
void LeafNode::clear() {
	for (int i = 0; i < m_KeyNum; ++i) {
		// if type of m_Datas is pointer
		//delete m_Datas[i];
		//m_Datas[i] = NULL;
	}
}

/**
 * @brief 分裂叶节点
 * 
 * @param parentNode 当前叶节点的父节点指针
 * @param childIndex 当前叶节点在父节点中对应键值的下标
 */
void LeafNode::split(BplusNode *parentNode, int childIndex) {
	LeafNode *newNode = new LeafNode();//分裂后的右节点
	setKeyNum(MINNUM_LEAF);
	newNode->setKeyNum(MINNUM_LEAF + 1);
	newNode->setRightSibling(getRightSibling());
	setRightSibling(newNode);
	newNode->setLeftSibling(this);
	int i;
	for (i =0 ; i < MINNUM_LEAF + 1; ++i) {
		// 拷贝关键字的值
		newNode->setKeyValue(i, m_KeyValues[i + MINNUM_LEAF]);
	}
	for (i = 0; i < MINNUM_LEAF + 1; ++i) {
		// 拷贝数据
		newNode->setData(i, m_Datas[i + MINNUM_LEAF]);
	}
	((InternalNode *)parentNode)->insert(childIndex, childIndex + 1, m_KeyValues[MINNUM_LEAF], newNode);
}

/**
 * @brief 合并叶节点
 * 
 * @param parentNode 叶节点的父节点指针
 * @param childNode 待合并的叶结点指针
 * @param keyIndex 该键值在父结点中对应的下标
 */
void LeafNode::mergeChild(BplusNode *parentNode, BplusNode *childNode, int keyIndex) {
	// 合并数据
	for (int i = 0; i < childNode->getKeyNum(); ++i)
		insert(childNode->getKeyValue(i), ((LeafNode*)childNode)->getData(i));
	setRightSibling(((LeafNode *)childNode)->getRightSibling());
	//父节点删除index的key，
	parentNode->removeKey(keyIndex, keyIndex + 1);
}
 
/**
 * @brief 根据叶结点的下标，从节点中移除键值
 */
void LeafNode::removeKey(int keyIndex, int childIndex) {
	for (int i = keyIndex; i < getKeyNum() - 1; ++i) {
		setKeyValue(i, getKeyValue(i + 1));
		setData(i, getData(i + 1));
	}
	setKeyNum(getKeyNum() - 1);
}

/**
 * @brief 从兄弟节点中借一个键值
 * 
 * @param siblingNode 被借的兄弟节点指针
 * @param parentNode 当前节点的父结点指针
 * @param keyIndex 需要填充的键值在该节点中的下标
 * @param d 被借的兄弟节点的相对位置
 */
void LeafNode::borrowFrom(BplusNode *siblingNode, BplusNode *parentNode, int keyIndex, SIBLING_DIRECTION d) {
	switch(d) {
		case LEFT: {
			// 从左兄弟结点借
			insert(siblingNode->getKeyValue(siblingNode->getKeyNum() - 1), ((LeafNode *)siblingNode)->getData(siblingNode->getKeyNum() - 1));
			siblingNode->removeKey(siblingNode->getKeyNum() - 1, siblingNode->getKeyNum() - 1);
			parentNode->setKeyValue(keyIndex, getKeyValue(0));
			break;
		}
		case RIGHT: {
			// 从右兄弟结点借
			insert(siblingNode->getKeyValue(0), ((LeafNode *)siblingNode)->getData(0));
			siblingNode->removeKey(0, 0);
			parentNode->setKeyValue(keyIndex, siblingNode->getKeyValue(0));
			break;
		}
		default:
			break;
	}
}
 
int LeafNode::getChildIndex(key_t key, int keyIndex) const { return keyIndex; }