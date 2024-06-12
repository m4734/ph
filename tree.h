
namespace PH
{

const size_t TREE_M = 8;

struct Tree_Node
{
	size_t key[TREE_M]; // -1
	void* node_p[TREE_M]; // tree node or leaf
	size_t len;
	Tree_Node* next;
};

struct Tree_Leaf
{
	void* next;
};

void tree_init();
void tree_clean();

Tree_Node* alloc_tree_node();
void free_tree_node(Tree_Node* node);

}
