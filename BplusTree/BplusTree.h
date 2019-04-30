#include <iostream>
#include <algorithm>
#include <vector>
#include "BplusNode.h"
#include "InternalNode.h"
#include "LeafNode.h"
#pragma once

#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H

using namespace std;

enum COMPARE_OPERATOR {
	LT, LE, EQ, GE, GT, BETWEEN
}; 	// 比较操作符：<、<=、=、>=、>、<>
 
struct SelectResult {
	int keyIndex;
	LeafNode *targetNode;
};
 
class BPlusTree {
public:
	BPlusTree() { m_Root = NULL; m_DataHead = NULL; }
	~BPlusTree() { clear(); }
	void clear();
	bool insert(key_t key, const tree_data_t &data);
	bool remove(key_t key, tree_data_t &dataValue);
	bool update(key_t oldKey, key_t newKey);
	bool search(key_t key);
	// 定值查询，compareOperator可以是LT(<)、LE(<=)、EQ(=)、BE(>=)、BT(>)
	vector<tree_data_t> select(key_t compareKey, int compareOpeartor);
	// 范围查询，BETWEEN
	vector<tree_data_t> select(key_t smallKey, key_t largeKey);
	// void printKey() const { printInConcavo(m_Root, 10); }
	void printData() const ;    // 打印数据
private:
	void recursive_insert(BplusNode *parentNode, key_t key, const tree_data_t &data);
	void recursive_remove(BplusNode *parentNode, key_t key, tree_data_t &dataValue);
	void search(key_t key, SelectResult &result);
	bool recursive_search(BplusNode *pNode, key_t key) const;
	void recursive_search(BplusNode *pNode, key_t key, SelectResult &result);
	void changeKey(BplusNode *pNode, key_t oldKey, key_t newKey);
	void printInConcavo(BplusNode *pNode, int count) const;

	// 成员变量
	BplusNode *m_Root;		// B+树的根节点
	LeafNode *m_DataHead; 	// 叶结点的头部
	key_t m_MaxKey;		// B+树中的最大键
};
 
#endif
// -----------------------------------------------------------------------
//                            Public Interfaces                           
// -----------------------------------------------------------------------
 
void BPlusTree::clear() {
	if (m_Root!=NULL) {
		m_Root->clear();
		delete m_Root;
		m_Root = NULL;
		m_DataHead = NULL;
	}
}

bool BPlusTree::insert(key_t key, const tree_data_t &data){
	// 是否已经存在
	if (search(key))
		return false;
		
	// 找到可以插入的叶子结点，否则创建新的叶子结点
	if(m_Root == NULL) {
		m_Root = new LeafNode();
		m_DataHead = (LeafNode*)m_Root;
		m_MaxKey = key;
	}
	if (m_Root->getKeyNum() >= MAXNUM_KEY) {
		// 根结点已满，分裂
		InternalNode *newNode = new InternalNode();  //创建新的根节点
		newNode->setChild(0, m_Root);
		m_Root->split(newNode, 0);
		m_Root = newNode;
	}
	if (key > m_MaxKey)
		// 更新最大键值
		m_MaxKey = key;
	
	recursive_insert(m_Root, key, data);
	return true;
}
 
bool BPlusTree::remove(key_t key, tree_data_t &dataValue) {
	if (!search(key))
		return false;
	if (m_Root->getKeyNum() == 1) {
		//特殊情况处理
		if (m_Root->getType() == LEAF) {
			clear();
			return true;
		} else {
			BplusNode *pChild1 = ((InternalNode *)m_Root)->getChild(0);
			BplusNode *pChild2 = ((InternalNode *)m_Root)->getChild(1);
			if (pChild1->getKeyNum() == MINNUM_KEY && pChild2->getKeyNum() == MINNUM_KEY) {
				pChild1->mergeChild(m_Root, pChild2, 0);
				delete m_Root;
				m_Root = pChild1;
			}
		}
	}
	recursive_remove(m_Root, key, dataValue);
	return true;
}
 
bool BPlusTree::update(key_t oldKey, key_t newKey) {
	if (search(newKey)) 
		// 检查更新后的键是否已经存在
		return false;
	else {
		tree_data_t dataValue;
		BPlusTree::remove(oldKey, dataValue);
		if (dataValue == INVALID_INDEX)
			return false;
		else
			return insert(newKey, dataValue);
	}
}
 
bool BPlusTree::search(key_t key) {
	return recursive_search(m_Root, key);
}
 
void BPlusTree::printData() const {
	LeafNode *itr = m_DataHead;
	while(itr) {
		for (int i = 0; i < itr->getKeyNum(); ++i)
			cout << itr->getKeyValue(i) << "->" << itr->getData(i) << " ";
		cout << endl;
		itr = itr->getRightSibling();
	}
}
 
vector<tree_data_t> BPlusTree::select(key_t compareKey, int compareOpeartor) {
	vector<tree_data_t> results;
	if (m_Root) {
		if (compareKey > m_MaxKey) {
			// 比较键值大于B+树中最大的键值
			if (compareOpeartor == LE || compareOpeartor == LT) {
				for(LeafNode *itr = m_DataHead; itr!=NULL; itr= itr->getRightSibling())
					for (int i = 0; i < itr->getKeyNum(); ++i)
						results.push_back(itr->getData(i));
			}
		} else if (compareKey < m_DataHead->getKeyValue(0)) {
			// 比较键值小于B+树中最小的键值
			if (compareOpeartor == GE || compareOpeartor == GT) {
				for(LeafNode *itr = m_DataHead; itr!=NULL; itr= itr->getRightSibling())
					for (int i = 0; i < itr->getKeyNum(); ++i)
						results.push_back(itr->getData(i));
			}
		} else {
			// 比较键值在B+树中
			SelectResult result;
			search(compareKey, result);
			switch(compareOpeartor) {
				case LE: {
					LeafNode *itr = m_DataHead;
					int i;
					while (itr!=result.targetNode) {
						for (i=0; i<itr->getKeyNum(); ++i)
							results.push_back(itr->getData(i));
						itr = itr->getRightSibling();
					}
					for (i=0; i<result.keyIndex; ++i)
						results.push_back(itr->getData(i));
					if (itr->getKeyValue(i) < compareKey || 
						(compareOpeartor==LE && compareKey==itr->getKeyValue(i)))
						results.push_back(itr->getData(i));
					break;
				}
				case EQ: {
					if (result.targetNode->getKeyValue(result.keyIndex)==compareKey)
					{
						results.push_back(result.targetNode->getData(result.keyIndex));
					}
					break;
				}
				case GT: {
					LeafNode *itr = result.targetNode;
					if (compareKey<itr->getKeyValue(result.keyIndex) ||
						(compareOpeartor==GE && compareKey==itr->getKeyValue(result.keyIndex)))
						results.push_back(itr->getData(result.keyIndex));
					int i;
					for (i=result.keyIndex+1; i<itr->getKeyNum(); ++i)
						results.push_back(itr->getData(i));
					itr = itr->getRightSibling();
					while (itr!=NULL) {
						for (i=0; i<itr->getKeyNum(); ++i)
							results.push_back(itr->getData(i));
						itr = itr->getRightSibling();
					}
					break;
				}
				default:  // 范围查询
					break;
			}
		}
	}
	sort<vector<tree_data_t>::iterator>(results.begin(), results.end());
	return results;
}
 
vector<tree_data_t> BPlusTree::select(key_t smallKey, key_t largeKey) {
	vector<tree_data_t> results;
	if (smallKey <= largeKey) {
		SelectResult start, end;
		search(smallKey, start);
		search(largeKey, end);
		LeafNode *itr = start.targetNode;
		int i = start.keyIndex;
		if (itr->getKeyValue(i) < smallKey)
			++i;
		if (end.targetNode->getKeyValue(end.keyIndex) > largeKey)
			--end.keyIndex;
		while (itr!=end.targetNode) {
			for (; i<itr->getKeyNum(); ++i)
				results.push_back(itr->getData(i));
			itr = itr->getRightSibling();
			i = 0;
		}
		for (; i<=end.keyIndex; ++i)
			results.push_back(itr->getData(i));
	}
	sort<vector<tree_data_t>::iterator>(results.begin(), results.end());
	return results;
}

// -----------------------------------------------------------------------
//                                 Private                                
// -----------------------------------------------------------------------

// 从上往下递归插入
void BPlusTree::recursive_insert(BplusNode *parentNode, key_t key, const tree_data_t &data) {
	if (parentNode->getType() == LEAF)  // 叶子结点
		((LeafNode*)parentNode)->insert(key, data);
	else {
		// 找到子结点
		int keyIndex = parentNode->getKeyIndex(key);
		int childIndex = parentNode->getChildIndex(key, keyIndex); // 孩子结点指针索引
		BplusNode *childNode = ((InternalNode*)parentNode)->getChild(childIndex);
		if (childNode->getKeyNum() >= MAXNUM_LEAF) {
			// 子结点已满，需进行分裂
			childNode->split(parentNode, childIndex);      
			if (parentNode->getKeyValue(childIndex) <= key)
				childNode = ((InternalNode *)parentNode)->getChild(childIndex + 1);
		}
		recursive_insert(childNode, key, data);
	}
}

// 从下往上递归删除
void BPlusTree::recursive_remove(BplusNode *parentNode, key_t key, tree_data_t &dataValue) {
	int keyIndex = parentNode->getKeyIndex(key);
	int childIndex= parentNode->getChildIndex(key, keyIndex);
	if (parentNode->getType() == LEAF) {
		// 找到目标叶子节点
		if (key == m_MaxKey && keyIndex > 0)
			m_MaxKey = parentNode->getKeyValue(keyIndex - 1);
		dataValue = ((LeafNode *)parentNode)->getData(keyIndex);
		parentNode->removeKey(keyIndex, childIndex);
		// 如果键值在内部结点中存在，也要相应的替换内部结点
		if (childIndex == 0 && m_Root->getType() != LEAF && parentNode != m_DataHead)
			changeKey(m_Root, key, parentNode->getKeyValue(0));
	} else {
		// 内结点 
		BplusNode *pChildNode = ((InternalNode*)parentNode)->getChild(childIndex); //包含key的子树根节点
		if (pChildNode->getKeyNum() == MINNUM_KEY) {
			// 包含键数量达到下限值，进行相关操作
			BplusNode *pLeft = childIndex>0 ? ((InternalNode*)parentNode)->getChild(childIndex-1) : NULL;                       //左兄弟节点
			BplusNode *pRight = childIndex<parentNode->getKeyNum() ? ((InternalNode*)parentNode)->getChild(childIndex+1) : NULL;//右兄弟节点
			if (pLeft && pLeft->getKeyNum()>MINNUM_KEY)
				// 左兄弟结点可借
				pChildNode->borrowFrom(pLeft, parentNode, childIndex-1, LEFT);
			else if (pRight && pRight->getKeyNum()>MINNUM_KEY)
				//右兄弟结点可借
				pChildNode->borrowFrom(pRight, parentNode, childIndex, RIGHT);
			else if (pLeft) {
				//与左兄弟合并
				pLeft->mergeChild(parentNode, pChildNode, childIndex-1);
				pChildNode = pLeft;
			} else if (pRight)
				//与右兄弟合并
				pChildNode->mergeChild(parentNode, pRight, childIndex);
		}
		recursive_remove(pChildNode, key, dataValue);
	}
}
 
void BPlusTree::search(key_t key, SelectResult &result) {
	recursive_search(m_Root, key, result);
}
 
bool BPlusTree::recursive_search(BplusNode *pNode, key_t key) const {
	if (pNode == NULL)  //检测节点指针是否为空，或该节点是否为叶子节点
		return false;
	else {
		int keyIndex = pNode->getKeyIndex(key);
		int childIndex = pNode->getChildIndex(key, keyIndex); // 孩子结点指针索引
		if (keyIndex < pNode->getKeyNum() && key == pNode->getKeyValue(keyIndex))
			return true;
		else {
			if (pNode->getType() == LEAF)
				//检查该节点是否为叶子节点
				return false;
			else
				return recursive_search(((InternalNode *)pNode)->getChild(childIndex), key);
		}
	}
}
 
void BPlusTree::recursive_search(BplusNode *pNode, key_t key, SelectResult &result) {
	int keyIndex = pNode->getKeyIndex(key);
	int childIndex = pNode->getChildIndex(key, keyIndex); // 孩子结点指针索引
	if (pNode->getType() == LEAF) {
		result.keyIndex = keyIndex;
		result.targetNode = (LeafNode*)pNode;
		return;
	}
	else
		return recursive_search(((InternalNode*)pNode)->getChild(childIndex), key, result);
}

void BPlusTree::changeKey(BplusNode *pNode, key_t oldKey, key_t newKey) {
	if (pNode!=NULL && pNode->getType() != LEAF) {
		int keyIndex = pNode->getKeyIndex(oldKey);
		if (keyIndex < pNode->getKeyNum() && oldKey == pNode->getKeyValue(keyIndex))
			pNode->setKeyValue(keyIndex, newKey);
		else
			changeKey(((InternalNode*)pNode)->getChild(keyIndex), oldKey, newKey);
	}
}
 
void BPlusTree::printInConcavo(BplusNode *pNode, int count) const {
	if (pNode != NULL) {
		int i, j;
		for (i = 0; i < pNode->getKeyNum(); ++i) {
			if (pNode->getType() != LEAF)
				printInConcavo(((InternalNode *)pNode)->getChild(i), count - 2);
			for (j=count; j>=0; --j)
				cout << "-";
			cout << pNode->getKeyValue(i) << endl;
		}
		if (pNode->getType()!=LEAF)
			printInConcavo(((InternalNode *)pNode)->getChild(i), count - 2);
	}
}
