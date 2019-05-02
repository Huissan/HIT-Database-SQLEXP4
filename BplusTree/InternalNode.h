#include "BplusNode.h"
#pragma once

#ifndef INTERNAL_NODE
#define INTERNAL_NODE


class InternalNode : public BplusNode{
public:
	InternalNode():BplusNode() { setType(INTERNAL); }
	virtual ~InternalNode() {}

	BplusNode *getChild(int i) const { return m_Childs[i]; }		// 获取子节点
	void setChild(int i, BplusNode *child) { m_Childs[i] = child; }	// 修改指向孩子节点的指针
	void insert(int keyIndex, int childIndex, key_t key, BplusNode *childNode);
    
	virtual void clear();
	virtual void split(BplusNode *parentNode, int childIndex);
	virtual void mergeChild(BplusNode *parentNode, BplusNode *childNode, int keyIndex);
	virtual void removeKey(int keyIndex, int childIndex);
	virtual void borrowFrom(BplusNode *destNode, BplusNode *parentNode, int keyIndex, SIBLING_DIRECTION d);
	virtual int getChildIndex(key_t key, int keyIndex) const ;
private:
	BplusNode *m_Childs[MAXNUM_CHILD];	// 指向孩子节点的指针
};

#endif // !INTERNAL_NODE

/**
 * @brief 向节点中插入值
 * 
 * @param keyIndex 待插入键值在当前节点的下标
 * @param childIndex 待插入键值的在子节点指针中的下标
 * @param key 待插入键值
 * @param childNode 指向子节点的指针
 */
void InternalNode::insert(int keyIndex, int childIndex, key_t key, BplusNode *childNode) {
	int i;
	for (i = getKeyNum(); i > keyIndex; --i) {
		// 将父节点中的childIndex后的所有关键字的值和子树指针向后移一位
		setChild(i + 1, m_Childs[i]);
		setKeyValue(i, m_KeyValues[i - 1]);
	}
	if (i == childIndex)
        setChild(i + 1, m_Childs[i]);
    setChild(childIndex, childNode);
	setKeyValue(keyIndex, key);
    setKeyNum(m_KeyNum + 1);
}

/**
 * @brief 清空所有的内节点
 */
void InternalNode::clear() {
	for (int i = 0; i <= m_KeyNum; ++i) {
		m_Childs[i]->clear();
		delete m_Childs[i];
		m_Childs[i] = 0;
	}
}

/**
 * @brief 分裂内节点
 * 
 * @param parentNode 当前内节点的父节点
 * @param childIndex 当前内节点在父节点中对应键值的下标
 */
void InternalNode::split(BplusNode *parentNode, int childIndex) {
	InternalNode *newNode = new InternalNode(); //分裂后的右节点
	newNode->setKeyNum(MINNUM_KEY);
	for (int i = 0; i < MINNUM_KEY; ++i) {
		// 拷贝关键字的值
		newNode->setKeyValue(i, m_KeyValues[i + MINNUM_CHILD]);
	}
	for (int i = 0; i < MINNUM_CHILD; ++i) {
		// 拷贝孩子节点指针
		newNode->setChild(i, m_Childs[i + MINNUM_CHILD]);
	}
	setKeyNum(MINNUM_KEY);  //更新左子树的关键字个数
	((InternalNode*)parentNode)->insert(childIndex, childIndex + 1, m_KeyValues[MINNUM_KEY], newNode);
}
 
/**
 * @brief 合并内节点
 * 
 * @param parentNode 当前节点的父节点
 * @param childNode 待合并的父节点的子节点
 * @param keyIndex 该键值在父结点中对应的下标
 */
void InternalNode::mergeChild(BplusNode *parentNode, BplusNode *childNode, int keyIndex) {
	// 合并数据
	insert(MINNUM_KEY, MINNUM_KEY+1, parentNode->getKeyValue(keyIndex), ((InternalNode*)childNode)->getChild(0));
	for (int i = 1; i <= childNode->getKeyNum(); ++i) {
        insert(MINNUM_KEY + i, MINNUM_KEY + i + 1, childNode->getKeyValue(i - 1), ((InternalNode *)childNode)->getChild(i));
    }
	//父节点删除index的key
	parentNode->removeKey(keyIndex, keyIndex + 1);
	delete ((InternalNode *)parentNode)->getChild(keyIndex + 1);
}

/**
 * @brief 从结点中移除键值
 * 
 * @param keyIndex 待移除键值的下标
 * @param childIndex 待移除的键值对应的子节点指针的下标
 */
void InternalNode::removeKey(int keyIndex, int childIndex) {
	for (int i = 0; i < (getKeyNum() - keyIndex - 1); ++i) {
		setKeyValue(keyIndex + i, getKeyValue(keyIndex + i + 1));
		setChild(childIndex + i, getChild(childIndex + i + 1));
	}
	setKeyNum(getKeyNum() - 1);
}

/**
 * @brief 从兄弟结点中借一个键值
 * 
 * @param siblingNode 当前节点的兄弟节点指针
 * @param parentNode 当前节点的父节点指针
 * @param keyIndex 需要填充的键值在该节点中的下标
 * @param d 被借的兄弟节点相对该节点的位置
 */
void InternalNode::borrowFrom(BplusNode *siblingNode, BplusNode *parentNode, int keyIndex, SIBLING_DIRECTION d) {
	switch(d) {
		case LEFT: {
			// 从左兄弟结点借
			insert(0, 0, parentNode->getKeyValue(keyIndex), ((InternalNode*)siblingNode)->getChild(siblingNode->getKeyNum()));
			parentNode->setKeyValue(keyIndex, siblingNode->getKeyValue(siblingNode->getKeyNum()-1));
			siblingNode->removeKey(siblingNode->getKeyNum()-1, siblingNode->getKeyNum());
			break;
		}
		case RIGHT: {
			// 从右兄弟结点借
            insert(getKeyNum(), getKeyNum() + 1, parentNode->getKeyValue(keyIndex), ((InternalNode *)siblingNode)->getChild(0));
            parentNode->setKeyValue(keyIndex, siblingNode->getKeyValue(0));
			siblingNode->removeKey(0, 0);
			break;
		}
		default:
			break;
	}
}

/**
 * @brief 根据条件获取子节点的下标
 * 
 * @param key 子节点的键值
 * @param keyIndex 子节点的
 * @return int 
 */
int InternalNode::getChildIndex(key_t key, int keyIndex) const {
	if (key == getKeyValue(keyIndex))
        return keyIndex + 1;
    else
		return keyIndex;
}