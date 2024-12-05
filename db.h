/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/
#include <cstdio>

#define MAX_IDENT_LEN 16
#define MAX_NUM_COL 16
#define MAX_TOK_LEN 32
#define KEYWORD_OFFSET 10
#define STRING_BREAK " (),<>="
#define NUMBER_BREAK " ),"
#define MAX_NUM_CONDITIONS 10
#define DELETE_FLAG_SIZE 1  // Delete flag is 1 byte


/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char col_name[MAX_IDENT_LEN + 4];
	int col_id; /* Start from 0 */
	int col_type;
	int col_len;
	int not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int tpd_size;
	char table_name[MAX_IDENT_LEN + 4];
	int num_columns;
	int cd_offset; /* Column Descriptor Offset */
	int tpd_flags; /* Column Packed Descriptor Flags */
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int list_size;
	int num_tables;
	int db_flags;
	tpd_entry tpd_start;
} tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked
	 together. */
typedef struct t_list
{
	char tok_string[MAX_TOK_LEN];
	int tok_class;
	int tok_value;
	struct t_list *next;
} token_list;

/* This struct is use to store table headers */
typedef struct tab_file_header_def
{
	int file_size;
	int record_size;
	int num_records;
	int record_offset;
	int file_header_flag;
	tpd_entry *tpd_ptr;
} table_file_header;

typedef struct {
    char **data;      // Array of strings, where each string represents a single row of data
    int num_rows;     // Number of rows in the result set
    int num_columns;  // Number of columns in the result set
} table_data;

typedef struct {
    char left_operand[MAX_TOK_LEN];
    char op[MAX_TOK_LEN];
    char right_operand[MAX_TOK_LEN];
} query_condition;

typedef struct {
    const char *table_name;   // Name of the table
    const char *column_name;  // Column name
    int col_offset;           // Offset of the column within the row
} column_mapping;


/* This enum defines the different classes of tokens for
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,   // 1
	identifier,	   // 2, Automatically initialized based on previous element
	symbol,		   // 3
	type_name,	   // 4
	constant,	   // 5
	function_name, // 6
	terminator,	   // 7
	error		   // 8
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing.

	T_<some_string>: Types
	K_<some_string>: Keywords
	S_<some_string>: Symbol
*/
typedef enum t_value
{
	T_INT = 10,		   // 10 - new type should be added above this line
	T_VARCHAR,		   // 11
	T_CHAR,			   // 12
	K_CREATE,		   // 13
	K_TABLE,		   // 14
	K_NOT,			   // 15
	K_NULL,			   // 16
	K_DROP,			   // 17
	K_LIST,			   // 18
	K_SCHEMA,		   // 19
	K_FOR,			   // 20
	K_TO,			   // 21
	K_INSERT,		   // 22
	K_INTO,			   // 23
	K_VALUES,		   // 24
	K_DELETE,		   // 25
	K_FROM,			   // 26
	K_WHERE,		   // 27
	K_UPDATE,		   // 28
	K_SET,			   // 29
	K_SELECT,		   // 30
	K_ORDER,		   // 31
	K_BY,			   // 32
	K_DESC,			   // 33
	K_IS,			   // 34
	K_AND,			   // 35
	K_OR,			   // 36 - new keyword should be added below this line
	F_SUM,			   // 37
	F_AVG,			   // 38
	F_COUNT,		   // 39 - new function name should be added below this line
	K_NATURAL,		   // 40
	K_JOIN,			   // 41
	K_ON,			   // 42
	S_LEFT_PAREN = 70, // 70
	S_RIGHT_PAREN,	   // 71
	S_COMMA,		   // 72
	S_STAR,			   // 73
	S_EQUAL,		   // 74
	S_LESS,			   // 75
	S_GREATER,		   // 76
	S_GREATER_EQUAL,   // 77
	S_LESS_EQUAL,	   // 78
	IDENT = 85,		   // 85
	INT_LITERAL = 90,  // 90
	STRING_LITERAL,	   // 91
	EOC = 95,		   // 95 End of Code?
	INVALID = 99	   // 99
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 33

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
char *keyword_table[] =
	{
		"int",
		"varchar",
		"char",
		"create",
		"table",
		"not",
		"null",
		"drop",
		"list",
		"schema",
		"for",
		"to",
		"insert",
		"into",
		"values",
		"delete",
		"from",
		"where",
		"update",
		"set",
		"select",
		"order",
		"by",
		"desc",
		"is",
		"and",
		"or",
		"sum",
		"avg",
		"count",
		"natural",
		"join",
		"on"};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
	INVALID_STATEMENT = -199, // -199
	CREATE_TABLE = 100,		  // 100
	DROP_TABLE,				  // 101
	LIST_TABLE,				  // 102
	LIST_SCHEMA,			  // 103
	INSERT,					  // 104
	DELETE,					  // 105
	UPDATE,					  // 106
	SELECT					  // 107
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399, // -399
	DUPLICATE_TABLE_NAME,	   // -398
	TABLE_NOT_EXIST,		   // -397
	INVALID_TABLE_DEFINITION,  // -396
	INVALID_COLUMN_NAME,	   // -395
	DUPLICATE_COLUMN_NAME,	   // -394
	COLUMN_NOT_EXIST,		   // -393
	MAX_COLUMN_EXCEEDED,	   // -392
	INVALID_TYPE_NAME,		   // -391
	INVALID_COLUMN_DEFINITION, // -390
	INVALID_COLUMN_LENGTH,	   // -389
	INVALID_REPORT_FILE_NAME,  // -388
							   /* Must add all the possible errors from I/U/D + SELECT here */
	FILE_OPEN_ERROR = -299,	   // -299
	DBFILE_CORRUPTION,		   // -298
	MEMORY_ERROR,			   // -297
	FILE_DELETE_ERROR,		   // -298
	STRING_TOO_LONG = -1999,
	TYPE_MISMATCH = -1899,
	SYNTAX_ERROR = -1799,
} return_codes;

/* Set of function prototypes */
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
/*sem_<some_function_name> relates to semantic processing */
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert_row(token_list *t_list);
int create_tab_file(tpd_entry *tpd);
int print_tab_file(char *table_name);
int is_null(const char *value);
int delete_tab_file(tpd_entry *tpd);
int sem_insert_row(const char *table_name, const char *row_data);

int sem_select_query_handler(token_list *tok_list);
void parse_select_all_query(token_list *tok_list,
							char *table_name,
							query_condition *conditions,
							int *num_conditions,
							char *logical_operators);
void parse_select_columns_query(token_list *tok_list,
								char *table_name,
								char **column_list,
								int *num_columns,
								query_condition *conditions,
								int *num_conditions,
								char *logical_operators);
void parse_inner_join_query(token_list *tok_list,
							char *table_name1,
							char *table_name2,
							char **column_list,
							int *num_columns,
							query_condition *conditions,
							int *num_conditions,
							char *logical_operators);
void handle_select_all(const char *table_name,
						query_condition *conditions,
						int num_conditions,
						char *logical_operators,
						const char *order_by_col,
						bool desc);
void handle_select_columns(const char *table_name,
							char **column_list,
							int num_columns,
							query_condition *conditions,
							int num_conditions,
							char *logical_operators,
							const char *order_by_col,
							bool desc);
void handle_select_inner_join(const char *table_name1,
								const char *table_name2,
								char **column_list,
								int num_columns,
                              	query_condition *conditions,
								int num_conditions,
								char *logical_operators,
                              	token_list *on_clause_tokens,
								const char *order_by_col,
								bool desc);
table_data *perform_inner_join(const char *table1,
								const char *table2,
								const char *join_col1,
								const char *join_col2,
								const char **select_columns,
								int num_columns,
								query_condition *conditions,
								int num_conditions,
								char *logical_operators
							);
int evaluate_conditions(const char *record,
						cd_entry *columns,
						query_condition *conditions,
						int num_conditions,
						char *logical_operators);
int evaluate_conditions_join(const char *record,
							 cd_entry *columns1, int num_columns1, int record_size1,
                             cd_entry *columns2, int num_columns2, int record_size2,
                             query_condition *conditions, int num_conditions, char *logical_operators);
char *merge_rows(const char *row1,
                 const char *row2,
                 const char **select_columns,
                 int num_columns,
                 cd_entry *columns1,
                 int num_columns1,
                 cd_entry *columns2,
                 int num_columns2);
void parse_on_clause(token_list *tok_list,
						char *join_col1,
						char *join_col2);
void parse_where_clause(token_list *tok_list,
						query_condition *conditions,
						int *num_conditions,
						char *logical_operators);
int is_select_all_query(token_list *tok_list);
int is_inner_join_query(token_list *tok_list);
token_list *extract_on_clause(token_list *tok_list);
void mark_row_deleted(FILE *file,
						int row_index,
						int record_size);
void fetch_and_print_records(const char *table_name,
								const char **column_list,
								int num_columns,
								query_condition *conditions,
								int num_conditions,
								char *logical_operators,
								const char *order_by_col,
								bool desc);
char **get_all_columns_from_table(const tpd_entry *tpd, int *num_columns);
void fetch_and_read_inner_join(const char *table_name1, const char *table_name2,
                               const char *join_col1, const char *join_col2,
                               column_mapping *validated_columns, int num_validated_columns,
                               const char **original_column_list, int num_columns,
                               query_condition *conditions, int num_conditions, char *logical_operators,
                               const char *order_by_col, bool desc);
void validate_columns_for_join(const char **column_list, int column_list_count, column_mapping *validated_columns,
                               int *validated_columns_count, const char *table_name1, const char *table_name2,
                               query_condition *conditions, int num_conditions, const char *order_by_col);
int sem_delete_row(token_list *t_list);
int sem_update_row(token_list *t_list);
void parse_order_by_clause(token_list *tok_list, char *order_by_column, bool *is_desc);
void quick_sort_rows(char **rows, int low, int high, int column_offset, int column_type, bool desc);
int partition_rows(char **rows, int low, int high, int column_offset, int column_type, bool desc);
int calculate_column_offset(const char *col_name, cd_entry *columns, int num_columns);
int calculate_column_offset_from_mapping(const char *col_name,
											column_mapping *columns,
											int num_columns,
											cd_entry *schema,
											int schema_columns);
int determine_column_type(const char *col_name,
							column_mapping *validated_columns,
							int num_validated_columns, 
                        	cd_entry *columns1,
							int num_columns1,
							cd_entry *columns2,
							int num_columns2);
bool column_in_list(const char *column_name, column_mapping *validated_columns, int num_validated_columns);
bool column_exists_in_table(const char *column_name, const char *table_name);
/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list *g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry *get_tpd_from_list(char *tabname);
