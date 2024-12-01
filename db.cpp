/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

int main(int argc, char **argv)
{
	int rc = 0;
	token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		// tok_ptr = tok_list;

		// printf("%16s \t%s \t %s\n", "token", "class", "value");
		// printf("===================================================\n");
		// while (tok_ptr != NULL)
		// {
		// 	printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
		// 		   tok_ptr->tok_value);
		// 	tok_ptr = tok_ptr->next;
		// }

		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr = tmp_tok_ptr;
		}
	}

	return rc;
}

/*************************************************************
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char *command, token_list **tok_list)
{
	int rc = 0, i, j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset((void *)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do
			{
				temp_string[i++] = *cur++;
			} while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET + j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET + j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET + j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, (char *)"", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do
			{
				temp_string[i++] = *cur++;
			} while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, (char *)"", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*') || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(':
				t_value = S_LEFT_PAREN;
				break;
			case ')':
				t_value = S_RIGHT_PAREN;
				break;
			case ',':
				t_value = S_COMMA;
				break;
			case '*':
				t_value = S_STAR;
				break;
			case '=':
				t_value = S_EQUAL;
				break;
			case '<':
				t_value = S_LESS;
				break;
			case '>':
				t_value = S_GREATER;
				break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, (char *)"", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do
			{
				temp_string[i++] = *cur++;
			} while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, (char *)"", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, (char *)"", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list *)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT ROW statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) &&
			 (cur->next != NULL))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch (cur_cmd)
		{
		case CREATE_TABLE:
			rc = sem_create_table(cur);
			break;
		case DROP_TABLE:
			rc = sem_drop_table(cur);
			break;
		case LIST_TABLE:
			rc = sem_list_tables();
			break;
		case LIST_SCHEMA:
			rc = sem_list_schema(cur);
			break;
		case INSERT:
			rc = sem_insert_row(cur);
			break;
		case SELECT:
			rc = sem_select_query_handler(cur);
			break;
		default:; /* no action */
		}
	}

	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	//	struct _stat file_stat;
	struct stat file_stat;

	/* Open for read */
	if ((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list *)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		// printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list *)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}
		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void *)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								   1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char *)cur + cur->tpd_size,
								   old_size - cur->tpd_size -
									   (sizeof(tpd_list) - sizeof(tpd_entry)),
								   1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char *)cur - (char *)g_tpd_list),
							   1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char *)g_tpd_list + g_tpd_list->list_size == (char *)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char *)cur + cur->tpd_size,
								   old_size - cur->tpd_size -
									   ((char *)cur - (char *)g_tpd_list),
								   1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry *)((char *)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry *get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry *)((char *)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

/*************************************************************
	Utility Function to Create .tab Binary File
 *************************************************************/
int create_tab_file(tpd_entry *tpd)
{
	FILE *tab_file;
	char filename[MAX_IDENT_LEN + 5]; // For "<table_name>.tab"
	int record_size = 0;
	table_file_header table_header;

	// Cconstruct filename for .tab file
	sprintf(filename, "%s.tab", tpd->table_name);

	// Open a new binary file
	tab_file = fopen(filename, "wb");
	if (!tab_file)
	{
		return FILE_OPEN_ERROR;
	}

	// Calculate record size based on table's schema
	cd_entry *col_entry = (cd_entry *)((char *)tpd + tpd->cd_offset);
	for (int i = 0; i < tpd->num_columns; i++, col_entry++)
	{
		if (col_entry->col_type == T_INT)
		{
			record_size += sizeof(int); // 4 bytes for INT
		}
		else if (col_entry->col_type == T_CHAR)
		{
			record_size += col_entry->col_len + 1; // CHAR(n) with 1-byte length prefix
		}
		// TODO: Add support for other data types
	}

	// Round the record size to a multiple of 4 for fast disk IO
	if (record_size % 4 != 0)
	{
		record_size += 4 - (record_size % 4);
	}

	// Initialize table file header
	memset(&table_header, 0, sizeof(table_file_header));
	table_header.file_size = sizeof(table_file_header);
	table_header.record_size = record_size;
	table_header.num_records = 0;
	table_header.record_offset = sizeof(table_file_header);
	table_header.file_header_flag = 0;
	table_header.tpd_ptr = tpd;

	// write header to .tab file
	fwrite(&table_header, sizeof(table_file_header), 1, tab_file);

	fclose(tab_file);
	return 0;
}

/*************************************************************
	Utility Function to Delete .tab Binary File
 *************************************************************/
int delete_tab_file(tpd_entry *tpd)
{
	char filename[MAX_IDENT_LEN + 5]; // +5 to accommodate ".tab\0"
	snprintf(filename, sizeof(filename), "%s.tab", tpd->table_name);

	// Remove the .tab file
	if (remove(filename) == 0)
	{
		printf("Table '%s' dropped successfully, and '%s' file deleted.\n", tpd->table_name, filename);
	}
	else
	{
		perror("Error deleting .tab file"); // Error message if deletion fails
		return FILE_DELETE_ERROR;			// Return an error code (define FILE_DELETE_ERROR as needed)
	}

	return 0;
}

/*************************************************************
	Handler to Create a new Table
 *************************************************************/
int sem_create_table(token_list *t_list) {
    int rc = 0;
    token_list *cur;
    tpd_entry tab_entry;
    tpd_entry *new_entry = NULL;
    bool column_done = false;
    int cur_id = 0;
    cd_entry col_entry[MAX_NUM_COL];

    memset(&tab_entry, '\0', sizeof(tpd_entry));
    cur = t_list;

    // Validate table name
    if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)) {
        fprintf(stderr, "Error: Invalid table name.\n");
        return INVALID_TABLE_NAME;
    }

    // Check for duplicate table name
    if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL) {
        fprintf(stderr, "Error: Duplicate table name '%s'.\n", cur->tok_string);
        return DUPLICATE_TABLE_NAME;
    }

    // Set table name
    strcpy(tab_entry.table_name, cur->tok_string);
    cur = cur->next;

    // Validate syntax: Expect '(' after table name
    if (cur->tok_value != S_LEFT_PAREN) {
        fprintf(stderr, "Error: Expected '(' after table name.\n");
        return INVALID_TABLE_DEFINITION;
    }
    cur = cur->next;

    // Initialize column entries
    memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

    /* Parse columns */
    do {
        // Validate column name
        if ((cur->tok_class != keyword) && (cur->tok_class != identifier) && (cur->tok_class != type_name)) {
            fprintf(stderr, "Error: Invalid column name.\n");
            return INVALID_COLUMN_NAME;
        }

        // Check for duplicate column name
        for (int i = 0; i < cur_id; i++) {
            if (strcmp(col_entry[i].col_name, cur->tok_string) == 0) {
                fprintf(stderr, "Error: Duplicate column name '%s'.\n", cur->tok_string);
                return DUPLICATE_COLUMN_NAME;
            }
        }

        // Set column name
        strcpy(col_entry[cur_id].col_name, cur->tok_string);
        col_entry[cur_id].col_id = cur_id;
        col_entry[cur_id].not_null = false;
        cur = cur->next;

        // Validate column type
        if (cur->tok_class != type_name) {
            fprintf(stderr, "Error: Invalid type for column '%s'.\n", col_entry[cur_id].col_name);
            return INVALID_TYPE_NAME;
        }
        col_entry[cur_id].col_type = cur->tok_value;
        cur = cur->next;

        // Handle CHAR type with length
        if (col_entry[cur_id].col_type == T_CHAR) {
            if (cur->tok_value != S_LEFT_PAREN) {
                fprintf(stderr, "Error: Expected '(' after CHAR for column '%s'.\n", col_entry[cur_id].col_name);
                return INVALID_COLUMN_DEFINITION;
            }
            cur = cur->next;

            if (cur->tok_value != INT_LITERAL) {
                fprintf(stderr, "Error: Expected length for CHAR column '%s'.\n", col_entry[cur_id].col_name);
                return INVALID_COLUMN_LENGTH;
            }
            col_entry[cur_id].col_len = atoi(cur->tok_string);
            cur = cur->next;

            if (cur->tok_value != S_RIGHT_PAREN) {
                fprintf(stderr, "Error: Expected ')' after CHAR length for column '%s'.\n", col_entry[cur_id].col_name);
                return INVALID_COLUMN_DEFINITION;
            }
            cur = cur->next;
        } else {
            // Default length for INT type
            col_entry[cur_id].col_len = sizeof(int);
        }

        // Handle NOT NULL constraint
        if (cur->tok_value == K_NOT && cur->next->tok_value == K_NULL) {
            col_entry[cur_id].not_null = true;
            cur = cur->next->next;
        }

        // Check for ',' or ')'
        if (cur->tok_value == S_COMMA) {
            cur = cur->next;
        } else if (cur->tok_value == S_RIGHT_PAREN) {
            column_done = true;
            cur = cur->next;
        } else {
            fprintf(stderr, "Error: Invalid column definition.\n");
            return INVALID_COLUMN_DEFINITION;
        }

        cur_id++;
    } while ((rc == 0) && (!column_done));

    // Validate end of query
    if ((column_done) && (cur->tok_value != EOC)) {
        fprintf(stderr, "Error: Expected end of query after ')'.\n");
        return INVALID_TABLE_DEFINITION;
    }

    /* Build the table packed descriptor (tpd) */
    tab_entry.num_columns = cur_id;
    tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) * tab_entry.num_columns;
    tab_entry.cd_offset = sizeof(tpd_entry);

    new_entry = (tpd_entry *)calloc(1, tab_entry.tpd_size);
    if (!new_entry) {
        fprintf(stderr, "Error: Memory allocation failed for table descriptor.\n");
        return MEMORY_ERROR;
    }

    memcpy((void *)new_entry, (void *)&tab_entry, sizeof(tpd_entry));
    memcpy((void *)((char *)new_entry + sizeof(tpd_entry)), (void *)col_entry, sizeof(cd_entry) * tab_entry.num_columns);

    // Add table descriptor to list
    rc = add_tpd_to_list(new_entry);
    if (!rc) {
        // Create table file
        rc = create_tab_file(new_entry);
    }

    free(new_entry);
    return rc;
}



/*************************************************************
	Handler to Drop a Table
 *************************************************************/
int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);

				// Delete .tab file
				rc = delete_tab_file(tab_entry);
			}
		}
	}

	return rc;
}

/*************************************************************
	Handler to List all tables
 *************************************************************/
int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry *)((char *)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

/*************************************************************
	Handler to List Schema
 *************************************************************/
int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN + 1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if ((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);
						}

						/* Next, write the cd_entry information */
						for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset);
							 i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

	return rc;
}

/*************************************************************
	Handler to Insert new Row
 *************************************************************/

int sem_insert_row(token_list *t_list) {
    char table_name[MAX_IDENT_LEN];
    token_list *cur = t_list;
	tpd_entry *tpd;

    // Step 1: Extract table name
    if ((cur->tok_class != keyword) && (cur->tok_class != identifier)) {
        fprintf(stderr, "Error: Invalid table name.\n");
        return INVALID_TABLE_NAME;
    }

	// Get table name and verify existence
	tpd = get_tpd_from_list(cur->tok_string);
	if (!tpd)
	{
		printf("Error: Table %s does not exist.\n", cur->tok_string);
		return TABLE_NOT_EXIST;
	}

    strcpy(table_name, cur->tok_string);
    cur = cur->next;

    // Step 2: Expect 'values' keyword
	if (cur == NULL || cur->tok_value != K_VALUES) {
        fprintf(stderr, "Error: Expected Keyword 'VALUES' after table name.\n");
        return SYNTAX_ERROR;
    }
    cur = cur->next;
	// Step 2: Validate syntax for '(' after table name
    if (cur == NULL || cur->tok_value != S_LEFT_PAREN) {
        fprintf(stderr, "Error: Expected '(' after 'VALUES' keyword.\n");
        return SYNTAX_ERROR;
    }
    cur = cur->next;

    // Step 3: Open the table file
    char filename[MAX_IDENT_LEN + 5];
    sprintf(filename, "%s.tab", table_name);

    FILE *file = fopen(filename, "r+b");
    if (!file) {
        fprintf(stderr, "Error: Could not open file %s\n", filename);
        return FILE_OPEN_ERROR;
    }

    // Step 4: Read table file header
    table_file_header header;
    fread(&header, sizeof(table_file_header), 1, file);

    // Step 5: Get column descriptors
    cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);

    // Step 6: Parse values
    char *record = (char *)malloc(header.record_size);
    memset(record, '\0', header.record_size);
    record[0] = 0; // Initialize delete flag to 0 (not deleted)

    for (int i = 0; i < tpd->num_columns; i++) {
        if (cur == NULL || cur->tok_value == S_RIGHT_PAREN) {
            fprintf(stderr, "Error: Missing value for column '%s'.\n", columns[i].col_name);
            free(record);
            fclose(file);
            return INVALID_COLUMN_DEFINITION;
        }

        if (columns[i].col_type == T_INT) {
            // Parse integer value
            if (cur->tok_value != INT_LITERAL) {
                fprintf(stderr, "Error: Expected integer value for column '%s'.\n", columns[i].col_name);
                free(record);
                fclose(file);
                return TYPE_MISMATCH;
            }
            int value = atoi(cur->tok_string);
			printf("Writing value: %d for column %s\n", value, columns[i].col_name);
            
			int offset = DELETE_FLAG_SIZE; // Start after the delete flag
			for (int j = 0; j < i; j++) {
				offset += columns[j].col_len; // Accumulate lengths of all preceding columns
			}

			memcpy(record + offset, &value, sizeof(int)); // Copy value to the calculated offset
			printf("Value written at offset %d\n", offset);
        } else if (columns[i].col_type == T_CHAR) {
            // Parse string value
            if (cur->tok_value != STRING_LITERAL) {
                fprintf(stderr, "Error: Expected string value for column '%s'.\n", columns[i].col_name);
                free(record);
                fclose(file);
                return TYPE_MISMATCH;
            }

			int offset = DELETE_FLAG_SIZE; // Start after the delete flag
			for (int j = 0; j < i; j++) {
				offset += columns[j].col_len; // Accumulate lengths of all preceding columns
			}

			// Copy the string value
			strncpy(record + offset, cur->tok_string, columns[i].col_len);

			// Ensure null termination if the string is shorter than the column length
			if (strlen(cur->tok_string) < columns[i].col_len) {
				memset(record + offset + strlen(cur->tok_string), '\0', columns[i].col_len - strlen(cur->tok_string));
			}
		}

        cur = cur->next;
        if (cur && cur->tok_value == S_COMMA) {
            cur = cur->next;
        }
    }

    if (cur == NULL || cur->tok_value != S_RIGHT_PAREN) {
        fprintf(stderr, "Error: Expected ')' at the end of values.\n");
        free(record);
        fclose(file);
        return SYNTAX_ERROR;
    }
    cur = cur->next;

    // Step 7: Find a deleted row slot or append at the end
    int row_index = -1;
    char flag;
    for (int i = 0; i < header.num_records; i++) {
        fseek(file, sizeof(table_file_header) + i * header.record_size, SEEK_SET);
        fread(&flag, DELETE_FLAG_SIZE, 1, file);
        if (flag == 1) { // Found a deleted row
            row_index = i;
            break;
        }
    }

    if (row_index == -1) {
        row_index = header.num_records;
        header.num_records++;
    }

    // Step 8: Write the new record
    fseek(file, sizeof(table_file_header) + row_index * header.record_size, SEEK_SET);
    fwrite(record, header.record_size, 1, file);

    // Step 9: Update table header
    fseek(file, 0, SEEK_SET);
    fwrite(&header, sizeof(table_file_header), 1, file);

    free(record);
    fclose(file);

    return 0;
}

/*************************************************************
	Utility Function to check if value is NULL as per
	database standards
 *************************************************************/
int is_null(const char *value)
{
	return value == NULL || strcmp(value, "") == 0 || strcmp(value, "NULL") == 0;
}


/*************************************************************
	Handler for Select Query
 *************************************************************/

int sem_select_query_handler(token_list *tok_list) {
    char table_name1[MAX_IDENT_LEN], table_name2[MAX_IDENT_LEN];
    char *column_list[MAX_NUM_COL];
    int num_columns = 0;

    // Declare conditions array and related variables
    query_condition conditions[MAX_NUM_CONDITIONS];
    int num_conditions = 0;
    char logical_operators[MAX_NUM_CONDITIONS];
    memset(logical_operators, 'A', sizeof(logical_operators)); // Default to AND

    if (is_inner_join_query(tok_list)) {
        token_list *on_clause_tokens = extract_on_clause(tok_list); // Extract ON clause tokens
        parse_inner_join_query(tok_list, table_name1, table_name2, column_list, &num_columns, conditions, &num_conditions, logical_operators);
        handle_select_inner_join(table_name1, table_name2, column_list, num_columns, conditions, num_conditions, logical_operators, on_clause_tokens);
    } else if (is_select_all_query(tok_list)) {
        parse_select_all_query(tok_list, table_name1, conditions, &num_conditions, logical_operators);
        handle_select_all(table_name1, conditions, num_conditions, logical_operators);
    } else {
        parse_select_columns_query(tok_list, table_name1, column_list, &num_columns, conditions, &num_conditions, logical_operators);
        handle_select_columns(table_name1, column_list, num_columns, conditions, num_conditions, logical_operators);
    }

    return 0;
}

void parse_select_all_query(token_list *tok_list, char *table_name, query_condition *conditions, int *num_conditions, char *logical_operators) {
    token_list *cur = tok_list;

    // Parse "SELECT * FROM"
    while (cur && cur->tok_value != K_FROM) {
        cur = cur->next;
    }
    if (!cur || cur->tok_value != K_FROM) {
        fprintf(stderr, "Error: Missing FROM clause.\n");
        return;
    }
    cur = cur->next;

    // Parse table name
    if (!cur || cur->tok_class != identifier) {
        fprintf(stderr, "Error: Expected table name after FROM.\n");
        return;
    }
    strncpy(table_name, cur->tok_string, MAX_IDENT_LEN);
    cur = cur->next;

    // Parse WHERE clause
    if (cur && cur->tok_value == K_WHERE) {
        cur = cur->next;
        parse_where_clause(cur, conditions, num_conditions, logical_operators);
    }
}

void parse_select_columns_query(token_list *tok_list, char *table_name, char **column_list, int *num_columns, query_condition *conditions, int *num_conditions, char *logical_operators) {
    token_list *cur = tok_list;

    // Parse column list
    while (cur && cur->tok_value != K_FROM) {
        if (cur->tok_class == identifier) {
            column_list[(*num_columns)++] = strdup(cur->tok_string);
        }
        cur = cur->next;
    }
    if (!cur || cur->tok_value != K_FROM) {
        fprintf(stderr, "Error: Missing FROM clause.\n");
        return;
    }
    cur = cur->next;

    // Parse table name
    if (!cur || cur->tok_class != identifier) {
        fprintf(stderr, "Error: Expected table name after FROM.\n");
        return;
    }
    strncpy(table_name, cur->tok_string, MAX_IDENT_LEN);
    cur = cur->next;

    // Parse WHERE clause
    if (cur && cur->tok_value == K_WHERE) {
        cur = cur->next;
        parse_where_clause(cur, conditions, num_conditions, logical_operators);
    }
}

void parse_inner_join_query(token_list *tok_list, char *table_name1, char *table_name2, char **column_list, int *num_columns, query_condition *conditions, int *num_conditions, char *logical_operators) {
    token_list *cur = tok_list;

    // Parse column list
    while (cur && cur->tok_value != K_FROM) {
        if (cur->tok_class == identifier) {
            column_list[(*num_columns)++] = strdup(cur->tok_string);
        }
        cur = cur->next;
    }
    if (!cur || cur->tok_value != K_FROM) {
        fprintf(stderr, "Error: Missing FROM clause.\n");
        return;
    }
    cur = cur->next;

    // Parse first table
    if (!cur || cur->tok_class != identifier) {
        fprintf(stderr, "Error: Expected table name after FROM.\n");
        return;
    }
    strncpy(table_name1, cur->tok_string, MAX_IDENT_LEN);
    cur = cur->next;

    if (!cur || cur->tok_value != K_NATURAL) {
        fprintf(stderr, "Error: Missing NATURAL JOIN clause.\n");
        return;
    }
    cur = cur->next;
	
	// Parse INNER JOIN
    if (!cur || cur->tok_value != K_JOIN) {
        fprintf(stderr, "Error: Missing JOIN Keyword for NATURAL JOIN.\n");
        return;
    }
    cur = cur->next;

    // Parse second table
    if (!cur || cur->tok_class != identifier) {
        fprintf(stderr, "Error: Expected second table name after JOIN.\n");
        return;
    }
    strncpy(table_name2, cur->tok_string, MAX_IDENT_LEN);
    cur = cur->next;

    // Parse ON clause
    if (!cur || cur->tok_value != K_ON) {
        fprintf(stderr, "Error: Missing ON clause.\n");
        return;
    }
    cur = cur->next;

    parse_where_clause(cur, conditions, num_conditions, logical_operators);
}

void handle_select_all(const char *table_name, query_condition *conditions, int num_conditions, char *logical_operators) {
    tpd_entry *tpd = get_tpd_from_list((char *)table_name);
	if (!tpd) {
		fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
		return;
	}

    // Get the column descriptors
    cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);

    // Dynamically build the column list
    char **column_list = (char **)malloc(tpd->num_columns * sizeof(char *));
    for (int i = 0; i < tpd->num_columns; i++) {
        column_list[i] = columns[i].col_name; // Point to each column name
    }

	// Call the fetch and print function
    fetch_and_print_records(table_name, (const char **)column_list, tpd->num_columns, conditions, num_conditions, logical_operators);

    // Free allocated resources
    free(column_list);

}

void handle_select_columns(const char *table_name, char **column_list, int num_columns, query_condition *conditions, int num_conditions, char *logical_operators) {
    // Fetch table metadata
    tpd_entry *tpd = get_tpd_from_list((char *)table_name);
    if (!tpd) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table_name);
        return;
    }

    cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);

    // Validate that all requested columns exist in the table
    for (int i = 0; i < num_columns; i++) {
        bool column_found = false;
        for (int j = 0; j < tpd->num_columns; j++) {
            if (strcmp(column_list[i], columns[j].col_name) == 0) {
                column_found = true;
                break;
            }
        }
        if (!column_found) {
            fprintf(stderr, "Error: Column '%s' does not exist in table '%s'.\n", column_list[i], table_name);
            return;
        }
    }

    // Call fetch_and_print_records with the specific columns
    fetch_and_print_records(table_name, (const char **)column_list, num_columns, conditions, num_conditions, logical_operators);
}

void handle_select_inner_join(const char *table_name1, const char *table_name2, char **column_list, int num_columns,
                              query_condition *conditions, int num_conditions, char *logical_operators,
                              token_list *on_clause_tokens) {
    // Declare variables for join columns
    char join_col1[MAX_TOK_LEN];
    char join_col2[MAX_TOK_LEN];

    // Parse the ON clause to extract join columns
    parse_on_clause(on_clause_tokens, join_col1, join_col2);

	column_mapping validated_columns[MAX_NUM_COL];
	validate_columns_for_join((const char **)column_list, num_columns, validated_columns, table_name1, table_name2);


    // Fetch and print the inner join results
    fetch_and_read_inner_join(table_name1, table_name2, join_col1, join_col2, validated_columns,
                              num_columns, conditions, num_conditions, logical_operators);
}

void validate_columns_for_join(const char **requested_columns, int num_requested_columns,
                               column_mapping *validated_columns, const char *table_name1,
                               const char *table_name2) {
    tpd_entry *tpd1 = get_tpd_from_list((char *)table_name1);
    tpd_entry *tpd2 = get_tpd_from_list((char *)table_name2);
    cd_entry *columns1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
    cd_entry *columns2 = (cd_entry *)((char *)tpd2 + tpd2->cd_offset);

    int col_index = 0;
    int offset = DELETE_FLAG_SIZE;

    // Validate columns for table 1
    for (int i = 0; i < num_requested_columns; i++) {
        for (int j = 0; j < tpd1->num_columns; j++) {
            if (strcmp(requested_columns[i], columns1[j].col_name) == 0) {
                validated_columns[col_index].table_name = table_name1;
                validated_columns[col_index].column_name = columns1[j].col_name;
                validated_columns[col_index].col_offset = offset;
                col_index++;
                break;
            }
            offset += columns1[j].col_len;
        }
    }

    offset = DELETE_FLAG_SIZE; // Reset for table 2

    // Validate columns for table 2
    for (int i = 0; i < num_requested_columns; i++) {
        for (int j = 0; j < tpd2->num_columns; j++) {
            if (strcmp(requested_columns[i], columns2[j].col_name) == 0) {
                validated_columns[col_index].table_name = table_name2;
                validated_columns[col_index].column_name = columns2[j].col_name;
                validated_columns[col_index].col_offset = offset;
                col_index++;
                break;
            }
            offset += columns2[j].col_len;
        }
    }
}



char **get_all_columns_from_table(const tpd_entry *tpd, int *num_columns) {
    cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);
    *num_columns = tpd->num_columns;

    char **column_list = (char **)malloc(*num_columns * sizeof(char *));
    for (int i = 0; i < *num_columns; i++) {
        column_list[i] = (char *)malloc(MAX_IDENT_LEN + 1);
        strcpy(column_list[i], columns[i].col_name);
    }

    return column_list;
}

void parse_on_clause(token_list *tok_list, char *join_col1, char *join_col2) {
    token_list *cur = tok_list;

    // Parse first join column
    if (!cur || cur->tok_class != identifier) {
        fprintf(stderr, "Error: Expected first join column.\n");
        return;
    }
    strncpy(join_col1, cur->tok_string, MAX_TOK_LEN);
    cur = cur->next;

    // Parse '=' operator
    if (!cur || cur->tok_value != S_EQUAL) {
        fprintf(stderr, "Error: Expected '=' in ON clause.\n");
        return;
    }
    cur = cur->next;

    // Parse second join column
    if (!cur || cur->tok_class != identifier) {
        fprintf(stderr, "Error: Expected second join column.\n");
        return;
    }
    strncpy(join_col2, cur->tok_string, MAX_TOK_LEN);
}

table_data *perform_inner_join(const char *table1, const char *table2, const char *join_col1, const char *join_col2, 
                               const char **select_columns, int num_columns, query_condition *conditions, 
                               int num_conditions, char *logical_operators) {
    // Retrieve metadata for table1
    tpd_entry *tpd1 = get_tpd_from_list((char *)table1);
    if (!tpd1) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table1);
        return NULL;
    }
    cd_entry *columns1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
    int num_columns1 = tpd1->num_columns;

    // Retrieve metadata for table2
    tpd_entry *tpd2 = get_tpd_from_list((char *)table2);
    if (!tpd2) {
        fprintf(stderr, "Error: Table '%s' not found.\n", table2);
        return NULL;
    }
    cd_entry *columns2 = (cd_entry *)((char *)tpd2 + tpd2->cd_offset);
    int num_columns2 = tpd2->num_columns;

    // Fetch records from both tables
    table_data *data1 = fetch_records(table1, NULL, 0, NULL, 0, NULL);
    table_data *data2 = fetch_records(table2, NULL, 0, NULL, 0, NULL);

    if (!data1 || !data2) {
        fprintf(stderr, "Error: Failed to fetch records from one or both tables.\n");
        if (data1) free(data1);
        if (data2) free(data2);
        return NULL;
    }

    // Prepare result table_data
    table_data *joined_data = (table_data *)malloc(sizeof(table_data));
    if (!joined_data) {
        fprintf(stderr, "Error: Memory allocation failed for joined data.\n");
        free(data1);
        free(data2);
        return NULL;
    }
    joined_data->num_rows = 0;
    joined_data->num_columns = num_columns;
    joined_data->data = (char **)malloc(data1->num_rows * data2->num_rows * sizeof(char *));
    if (!joined_data->data) {
        fprintf(stderr, "Error: Memory allocation failed for joined data rows.\n");
        free(joined_data);
        free(data1);
        free(data2);
        return NULL;
    }

    // Calculate offsets for join columns
    int join_offset1 = find_column_offset(join_col1, columns1, num_columns1);
    int join_offset2 = find_column_offset(join_col2, columns2, num_columns2);
    if (join_offset1 == -1 || join_offset2 == -1) {
        fprintf(stderr, "Error: Join column not found in one or both tables.\n");
        free(joined_data->data);
        free(joined_data);
        free(data1);
        free(data2);
        return NULL;
    }

    // Perform the INNER JOIN logic
    for (int i = 0; i < data1->num_rows; i++) {
        for (int j = 0; j < data2->num_rows; j++) {
            // Compare the join column values
            if (strncmp(data1->data[i] + join_offset1, data2->data[j] + join_offset2, MAX_TOK_LEN) == 0) {
                // Merge rows and add to joined_data
                joined_data->data[joined_data->num_rows++] = merge_rows(
                    data1->data[i],
                    data2->data[j],
                    select_columns,
                    num_columns,
                    columns1,
                    num_columns1,
                    columns2,
                    num_columns2
                );
            }
        }
    }

    // Free resources for data1 and data2
    for (int i = 0; i < data1->num_rows; i++) {
        free(data1->data[i]);
    }
    free(data1->data);
    free(data1);

    for (int i = 0; i < data2->num_rows; i++) {
        free(data2->data[i]);
    }
    free(data2->data);
    free(data2);

    return joined_data;
}

void fetch_and_read_inner_join(const char *table_name1, const char *table_name2,
                               const char *join_col1, const char *join_col2,
                               column_mapping *validated_columns, int num_columns,
                               query_condition *conditions, int num_conditions, char *logical_operators) {
    // Fetch metadata for both tables
    tpd_entry *tpd1 = get_tpd_from_list((char *)table_name1);
    tpd_entry *tpd2 = get_tpd_from_list((char *)table_name2);

    if (!tpd1 || !tpd2) {
        fprintf(stderr, "Error: One or both tables not found.\n");
        return;
    }

    // Open table files to determine their sizes
    char filename1[MAX_IDENT_LEN + 5];
    char filename2[MAX_IDENT_LEN + 5];
    sprintf(filename1, "%s.tab", table_name1);
    sprintf(filename2, "%s.tab", table_name2);

    FILE *file1 = fopen(filename1, "rb");
    FILE *file2 = fopen(filename2, "rb");

    if (!file1 || !file2) {
        fprintf(stderr, "Error: Failed to open one or both table files.\n");
        if (file1) fclose(file1);
        if (file2) fclose(file2);
        return;
    }

    // Read table headers to compare sizes
    table_file_header header1, header2;
    fread(&header1, sizeof(table_file_header), 1, file1);
    fread(&header2, sizeof(table_file_header), 1, file2);

    // Ensure larger table is on the left-hand side
    if (header2.num_records > header1.num_records) {
        // Swap tables and join columns
        const char *temp_table = table_name1;
        table_name1 = table_name2;
        table_name2 = temp_table;

        const char *temp_col = join_col1;
        join_col1 = join_col2;
        join_col2 = temp_col;

        // Swap file handles and headers
        FILE *temp_file = file1;
        file1 = file2;
        file2 = temp_file;

        table_file_header temp_header = header1;
        header1 = header2;
        header2 = temp_header;

        tpd_entry *temp_tpd = tpd1;
        tpd1 = tpd2;
        tpd2 = temp_tpd;
    }

    cd_entry *columns1 = (cd_entry *)((char *)tpd1 + tpd1->cd_offset);
    cd_entry *columns2 = (cd_entry *)((char *)tpd2 + tpd2->cd_offset);

    // Print column headers
    for (int i = 0; i < num_columns; i++) {
        printf("%-30s | ", validated_columns[i].column_name);
    }
    printf("\n");
    for (int i = 0; i < num_columns; i++) {
        printf("-------------------------------+ ");
    }
    printf("\n");

    // Read rows from both tables and perform join
    char *record1 = (char *)malloc(header1.record_size);
    char *record2 = (char *)malloc(header2.record_size);

    int join_offset1 = find_column_offset(join_col1, columns1, tpd1->num_columns);
    int join_offset2 = find_column_offset(join_col2, columns2, tpd2->num_columns);

    for (int i = 0; i < header1.num_records; i++) {
        fseek(file1, header1.record_offset + i * header1.record_size, SEEK_SET);
        fread(record1, header1.record_size, 1, file1);

        if (record1[0] == 1) { // Skip deleted rows
            continue;
        }

        for (int j = 0; j < header2.num_records; j++) {
            fseek(file2, header2.record_offset + j * header2.record_size, SEEK_SET);
            fread(record2, header2.record_size, 1, file2);

            if (record2[0] == 1) { // Skip deleted rows
                continue;
            }

            // Check join condition
            if (strncmp(record1 + join_offset1, record2 + join_offset2, MAX_TOK_LEN) == 0) {
                // Concatenate rows for condition evaluation
                char *combined_row = (char *)malloc(header1.record_size + header2.record_size);
                memcpy(combined_row, record1, header1.record_size);
                memcpy(combined_row + header1.record_size, record2, header2.record_size);

                // Evaluate additional conditions
                if (evaluate_conditions(combined_row, columns1, conditions, num_conditions, logical_operators)) {
                    // Print selected columns for joined rows
                    for (int k = 0; k < num_columns; k++) {
                        if (strcmp(validated_columns[k].table_name, table_name1) == 0) {
                            int col_offset = find_column_offset(validated_columns[k].column_name, columns1, tpd1->num_columns);
                            printf("%-30s | ", record1 + col_offset);
                        } else if (strcmp(validated_columns[k].table_name, table_name2) == 0) {
                            int col_offset = find_column_offset(validated_columns[k].column_name, columns2, tpd2->num_columns);
                            printf("%-30s | ", record2 + col_offset);
                        }
                    }
                    printf("\n");
                }

                free(combined_row);
            }
        }
    }

    // Free resources
    free(record1);
    free(record2);
    fclose(file1);
    fclose(file2);
}


table_data *fetch_records(const char *table_name, const char **column_list, int num_columns, query_condition *conditions, int num_conditions, char *logical_operators) {
    // Open table file
	int rc;
    char filename[MAX_IDENT_LEN + 5];
    sprintf(filename, "%s.tab", table_name);

    FILE *file = fopen(filename, "rb");
    if (!file) {
        fprintf(stderr, "Error: Could not open file %s\n", filename);
        return NULL;
    }

    table_file_header header;
    fread(&header, sizeof(table_file_header), 1, file);

	tpd_entry *tpd;
	tpd = get_tpd_from_list((char *)table_name);
	if(!tpd){
		printf("Error: Table %s does not exist.\n", table_name);
		rc = TABLE_NOT_EXIST;
		// TODO: Manage how to return table not found error
	}

    // cd_entry *columns = (cd_entry *)((char *)header.tpd_ptr + header.tpd_ptr->cd_offset);
	cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);

    // Prepare data structure for the results
    table_data *data = (table_data *)malloc(sizeof(table_data));
    data->num_rows = 0;
    data->num_columns = num_columns;
    data->data = (char **)malloc(header.num_records * sizeof(char *));

    char *record = (char *)malloc(header.record_size);

    for (int i = 0; i < header.num_records; i++) {
        fseek(file, sizeof(table_file_header) + i * header.record_size, SEEK_SET);
        fread(record, header.record_size, 1, file);

        if (record[0] == 1) { // Skip deleted rows
            continue;
        }

        if (evaluate_conditions(record + DELETE_FLAG_SIZE, columns, conditions, num_conditions, logical_operators)) {

			int row_size = DELETE_FLAG_SIZE;
            for (int j = 0; j < num_columns; j++) {
                row_size += columns[j].col_len;
            }
			data->data[data->num_rows] = (char *)malloc(row_size);
			if (!data->data[data->num_rows]) {
				fprintf(stderr, "Error: Memory allocation failed for row %d.\n", data->num_rows);
				exit(EXIT_FAILURE);
			}

			int col_offset = DELETE_FLAG_SIZE;
            for (int j = 0; j < num_columns; j++) {
                strncpy(data->data[data->num_rows] + (j * MAX_TOK_LEN), record + col_offset, columns[j].col_len);
                col_offset += columns[j].col_len;

                // Null terminate strings
                if (columns[j].col_type == T_CHAR || columns[j].col_type == T_VARCHAR) {
                    data->data[data->num_rows][(j + 1) * MAX_TOK_LEN - 1] = '\0';
                }
            }

            data->num_rows++;
        }
    }

    free(record);
    fclose(file);
    return data;
}

void mark_row_deleted(FILE *file, int row_index, int record_size) {
    fseek(file, sizeof(table_file_header) + row_index * record_size, SEEK_SET);
    char flag = 1;  // Mark as deleted
    fwrite(&flag, DELETE_FLAG_SIZE, 1, file);
}

void print_table(const char **column_list, int num_columns, const table_data *data) {
    if (column_list != NULL) {
        for (int i = 0; i < num_columns; i++) {
            printf("%-30s | ", column_list[i]); // Print column headers
        }
        printf("\n");
        for (int i = 0; i < num_columns; i++) {
            printf("-------------------------------+ ");
        }
        printf("\n");
    }

	for (int i = 0; i < data->num_rows; i++) {
		if (!data->data[i]) {
			fprintf(stderr, "Error: Row %d is NULL\n", i);
			continue;
		}
		for (int j = 0; j < num_columns; j++) {
			printf("%-30s | ", data->data[i] + (j * MAX_TOK_LEN));
		}
		printf("\n");
	}
}

void fetch_and_print_records(const char *table_name, const char **column_list, int num_columns, query_condition *conditions, int num_conditions, char *logical_operators) {
    // Open the table file
	int rc=0;
    char filename[MAX_IDENT_LEN + 5];
    sprintf(filename, "%s.tab", table_name);

    FILE *file = fopen(filename, "rb");
    if (!file) {
        fprintf(stderr, "Error: Could not open file %s\n", filename);
        return;
    }

    table_file_header header;
    fread(&header, sizeof(table_file_header), 1, file);

	tpd_entry *tpd;
	tpd = get_tpd_from_list((char *)table_name);
	if(!tpd){
		printf("Error: Table %s does not exist.\n", table_name);
		rc = TABLE_NOT_EXIST;
		// TODO: Manage how to return table not found error
	}

    // cd_entry *columns = (cd_entry *)((char *)header.tpd_ptr + header.tpd_ptr->cd_offset);
    cd_entry *columns = (cd_entry *)((char *)tpd + tpd->cd_offset);

    // Print column headers
    if (column_list != NULL) {
        for (int i = 0; i < num_columns; i++) {
            printf("%-30s | ", column_list[i]);
        }
        printf("\n");
        for (int i = 0; i < num_columns; i++) {
            printf("-------------------------------+ ");
        }
        printf("\n");
    }

    // Allocate space for a record
    char *record = (char *)malloc(header.record_size);

    // Iterate through records
    for (int i = 0; i < header.num_records; i++) {
        fseek(file, sizeof(table_file_header) + i * header.record_size, SEEK_SET);
        fread(record, header.record_size, 1, file);

        if (record[0] == 1) { // Skip deleted rows
            continue;
        }

        if (evaluate_conditions(record + DELETE_FLAG_SIZE, columns, conditions, num_conditions, logical_operators)) {
            // Print each column for the matching row
            int col_offset = DELETE_FLAG_SIZE;
			for (int j = 0; j < num_columns; j++) {
				if (columns[j].col_type == T_INT) {
					// Print integer column
					printf("%-30d | ", *((int *)(record + col_offset)));
				} else if (columns[j].col_type == T_CHAR || columns[j].col_type == T_VARCHAR) {
					// Print string column (ensure null termination)
					printf("%-30.*s | ", columns[j].col_len, record + col_offset);
				}
				col_offset += columns[j].col_len; // Move to next column
			}
			printf("\n");
        }
    }

    // Free allocated resources
    if (record) {
		free(record);
		record = NULL; // Prevent double free
	}

    fclose(file);
}


int evaluate_conditions(const char *record, cd_entry *columns, query_condition *conditions, int num_conditions, char *logical_operators) {
    
	if (num_conditions == 0) {
        return 1; // No conditions mean all rows satisfy the query
    }

	int result = (logical_operators[0] == 'O') ? 0 : 1; // Initial result based on the first operator (AND/OR)

    for (int i = 0; i < num_conditions; i++) {
        query_condition *cond = &conditions[i];
        const char *col_name = cond->left_operand;
        const char *op = cond->op;
        const char *value = cond->right_operand;

        // Find the column in the schema
        int offset = 0;
        cd_entry *target_col = NULL;
        for (int j = 0; j < MAX_NUM_COL; j++) {
            if (strcmp(columns[j].col_name, col_name) == 0) {
                target_col = &columns[j];
                break;
            }
            offset += columns[j].col_len;
        }

        if (!target_col) {
            fprintf(stderr, "Error: Column '%s' not found.\n", col_name);
            return 0; // Column not found
        }

        // Access the value at the calculated offset
        const char *field = record + offset;
        int condition_result = 0;
        if (target_col->col_type == T_INT) {
            int record_value = *(int *)field;
            int condition_value = atoi(value);

            if ((strcmp(op, "=") == 0 && record_value == condition_value) ||
                (strcmp(op, "<") == 0 && record_value < condition_value) ||
                (strcmp(op, ">") == 0 && record_value > condition_value) ||
                (strcmp(op, "<=") == 0 && record_value <= condition_value) ||
                (strcmp(op, ">=") == 0 && record_value >= condition_value)) {
                condition_result = 1;
            }
        } else if (target_col->col_type == T_CHAR) {
            char record_value[MAX_TOK_LEN + 1];
            strncpy(record_value, field, target_col->col_len);
            record_value[target_col->col_len] = '\0';

            if ((strcmp(op, "=") == 0 && strcmp(record_value, value) == 0) ||
                (strcmp(op, "<") == 0 && strcmp(record_value, value) < 0) ||
                (strcmp(op, ">") == 0 && strcmp(record_value, value) > 0) ||
                (strcmp(op, "<=") == 0 && strcmp(record_value, value) <= 0) ||
                (strcmp(op, ">=") == 0 && strcmp(record_value, value) >= 0)) {
                condition_result = 1;
            }
        }

        // Combine condition results using logical operators
        if (logical_operators[i] == 'A') {
            result = result && condition_result; // AND
        } else if (logical_operators[i] == 'O') {
            result = result || condition_result; // OR
        }
    }

    return result;
}

char *merge_rows(const char *row1,
					const char *row2,
					const char **select_columns,
					int num_columns,
					cd_entry *columns1,
					int num_columns1,
					cd_entry *columns2,
					int num_columns2) {
    // Allocate memory for the merged row
    char *merged_row = (char *)malloc(num_columns * MAX_TOK_LEN);
    if (!merged_row) {
        fprintf(stderr, "Error: Memory allocation failed for merged row.\n");
        return NULL;
    }

    for (int i = 0; i < num_columns; i++) {
        const char *col_name = select_columns[i];
        const char *value = NULL;

        // Check if the column is in table1
        int offset = 0;
        for (int j = 0; j < num_columns1; j++) {
            if (strcmp(columns1[j].col_name, col_name) == 0) {
                value = row1 + offset;
                strncpy(merged_row + i * MAX_TOK_LEN, value, columns1[j].col_len);
                merged_row[i * MAX_TOK_LEN + columns1[j].col_len] = '\0'; // Null-terminate
                break;
            }
            offset += columns1[j].col_len;
        }

        // If not found in table1, check table2
        if (!value) {
            offset = 0;
            for (int j = 0; j < num_columns2; j++) {
                if (strcmp(columns2[j].col_name, col_name) == 0) {
                    value = row2 + offset;
                    strncpy(merged_row + i * MAX_TOK_LEN, value, columns2[j].col_len);
                    merged_row[i * MAX_TOK_LEN + columns2[j].col_len] = '\0'; // Null-terminate
                    break;
                }
                offset += columns2[j].col_len;
            }
        }

        // If column is not found in either table, fill with an empty string
        if (!value) {
            strncpy(merged_row + i * MAX_TOK_LEN, "", MAX_TOK_LEN);
        }
    }

    return merged_row;
}

int find_column_offset(const char *col_name, cd_entry *columns, int num_columns) {
    int offset = DELETE_FLAG_SIZE;

    for (int i = 0; i < num_columns; i++) {
        if (strcmp(columns[i].col_name, col_name) == 0) {
            return offset; // Found column, return its offset
        }
        offset += columns[i].col_len; // Increment offset by column length
    }

    return -1; // Column not found
}

void parse_where_clause(token_list *tok_list, query_condition *conditions, int *num_conditions, char *logical_operators) {
    token_list *cur = tok_list;

    // Iterate through tokens in the WHERE clause
    while (cur != NULL) {
        if (cur->tok_class == identifier) {
            // Parse left operand (column name)
            strncpy(conditions[*num_conditions].left_operand, cur->tok_string, MAX_TOK_LEN);
            cur = cur->next;

            // Parse operator
            if (cur == NULL || (cur->tok_value != S_EQUAL && cur->tok_value != S_GREATER &&
                                cur->tok_value != S_LESS && cur->tok_value != S_GREATER_EQUAL &&
                                cur->tok_value != S_LESS_EQUAL)) {
                fprintf(stderr, "Error: Expected a valid operator in WHERE clause.\n");
                return;
            }
            strncpy(conditions[*num_conditions].op, cur->tok_string, MAX_TOK_LEN);
            cur = cur->next;

            // Parse right operand (value)
            if (cur == NULL || (cur->tok_class != constant && cur->tok_class != identifier)) {
                fprintf(stderr, "Error: Expected a constant or identifier as right operand.\n");
                return;
            }
            strncpy(conditions[*num_conditions].right_operand, cur->tok_string, MAX_TOK_LEN);
            cur = cur->next;

            // Increment the number of conditions parsed
            (*num_conditions)++;
        }

        // Parse logical operators (AND/OR)
        if (cur != NULL && (cur->tok_value == K_AND || cur->tok_value == K_OR)) {
            logical_operators[*num_conditions - 1] = (cur->tok_value == K_AND) ? 'A' : 'O';
            cur = cur->next;
        } else if (cur != NULL && cur->tok_class == terminator) {
            break; // End of WHERE clause
        }
    }
}

int is_select_all_query(token_list *tok_list) {
    token_list *cur = tok_list;

    // Check for "*"
    if (!cur || cur->tok_value != S_STAR) {
        return 0; // Not a SELECT * query
    }
    cur = cur->next;

    // Check for "FROM"
    if (!cur || cur->tok_value != K_FROM) {
        return 0; // Not a SELECT * query
    }

    return 1; // It is a SELECT * query
}
int is_inner_join_query(token_list *tok_list) {
    token_list *cur = tok_list;

    // Skip column list (or wildcard)
    while (cur && cur->tok_value != K_FROM) {
        cur = cur->next;
    }

    // Check for "FROM"
    if (!cur || cur->tok_value != K_FROM) {
        return 0; // Not a SELECT query with a FROM clause
    }
    cur = cur->next;

    // Skip table name or alias
    if (!cur || cur->tok_class != identifier) {
        return 0; // Missing table name
    }
    cur = cur->next;

	// Check for "NATURAL JOIN"
    if (!cur || cur->tok_value != K_NATURAL) {
        return 0; // No INNER JOIN
    }
	cur = cur->next;

    // Check for "INNER JOIN"
    if (!cur || cur->tok_value != K_JOIN) {
        return 0; // No INNER JOIN
    }

    return 1; // It is a query with INNER JOIN
}

token_list *extract_on_clause(token_list *tok_list) {
    token_list *cur = tok_list;

    // Traverse tokens to find "ON" keyword
    while (cur != NULL) {
        if (cur->tok_value == K_ON) {
            return cur->next; // Return the tokens after "ON"
        }
        cur = cur->next;
    }

    fprintf(stderr, "Error: ON clause not found in INNER JOIN query.\n");
    return NULL;
}
