#include "BplusNode.h"
#pragma once

#ifndef INTERNAL_NODE
#define INTERNAL_NODE


class InternalNode : public BplusNode{
public:
	InternalNode():BplusNode() { setType(INTERNAL); }
	virtual ~InternalNode() {}
 
	BplusNode *getChild(int i) const { return m_Childs[i]; }
	// 修改指向孩子节点的指针
	void setChild(int i, BplusNode *child){ m_Childs[i] = child; }
	// 插入一个内节点
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

// 向节点中插入值
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

void InternalNode::clear() {
	for (int i = 0; i <= m_KeyNum; ++i) {
		m_Childs[i]->clear();
		delete m_Childs[i];
		m_Childs[i] = 0;
	}
}

// 分裂节点
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
 
// 合并节点
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

// 从结点中移除键值
void InternalNode::removeKey(int keyIndex, int childIndex) {
	for (int i = 0; i < (getKeyNum() - keyIndex - 1); ++i) {
		setKeyValue(keyIndex + i, getKeyValue(keyIndex + i + 1));
		setChild(childIndex + i, getChild(childIndex + i + 1));
	}
	setKeyNum(getKeyNum() - 1);
}

// 从兄弟结点中借一个键值
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

// 
int InternalNode::getChildIndex(key_t key, int keyIndex) const {
	if (key == getKeyValue(keyIndex))
        return keyIndex + 1;
    else
		return keyIndex;
}