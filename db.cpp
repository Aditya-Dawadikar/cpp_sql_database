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

int main(int argc, char** argv){
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

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
		tok_ptr = tok_list;
		
		printf("%16s \t%s \t %s\n","token","class","value");
		printf("===================================================\n");
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
	
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
			tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list){
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  	memset ((void*)temp_string, '\0', MAX_TOK_LEN);
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
			}
			while ((isalnum(*cur)) || (*cur == '_'));

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
					if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            			t_class = function_name;
          			else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
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
					add_to_list(tok_list, (char *) "", terminator, EOC);
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
			}
			while (isdigit(*cur));

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
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
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
			}
			while ((*cur) && (*cur != '\''));

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
				add_to_list(tok_list, (char*)"", terminator, EOC);
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

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value){
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
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

int do_semantic(token_list *tok_list){
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
		switch(cur_cmd)
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
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list){
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
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
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              				/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  	tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						// Create Table File
						if (!rc){
							rc = create_tab_file(new_entry);
						}

						free(new_entry);
					}
				}
			}
		}
	}
  return rc;
}

int sem_drop_table(token_list *t_list){
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
			}
		}
	}

  return rc;
}

int sem_list_tables(){
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
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list){
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
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
			memset(filename, '\0', MAX_IDENT_LEN+1);
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
						if((fhandle = fopen(filename, "a+tc")) == NULL)
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
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
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

int sem_insert_row(token_list *t_list){
	// printf("about to start insert operation\n");

    token_list *cur = t_list;
    tpd_entry *tpd;
    cd_entry *col_entry;
    FILE *tab_file;
    char filename[MAX_IDENT_LEN + 5];
    table_file_header table_header;
    char *record_buffer, *record_ptr;
    int record_size, num_columns;
    bool column_names_present = false;

    // Step 1: Expect table name
    if (cur->tok_value != IDENT)
    {
		perror("Table name not found in token list\n");
        return INVALID_STATEMENT;
    }

    // Get table name and verify existence
    tpd = get_tpd_from_list(cur->tok_string);
    if (!tpd)
    {
        printf("Error: Table %s does not exist.\n", cur->tok_string);
        return TABLE_NOT_EXIST;
    }

    // Step 2: Construct the .tab filename
    sprintf(filename, "%s.tab", cur->tok_string);
    cur = cur->next;

    // Step 3: Expect 'values' keyword
    if (cur->tok_value != K_VALUES)
    {
		perror("keyword VALUES not found in token list\n");
        return INVALID_STATEMENT;
    }
    cur = cur->next;

    // Step 4: Expect '('
    if (cur->tok_value != S_LEFT_PAREN)
    {
		perror("left parenthesis not found in token list\n");
        return INVALID_STATEMENT;
    }
    cur = cur->next;

    // Step 5: Open the .tab file and read the header
    tab_file = fopen(filename, "r+b");
    if (!tab_file)
    {
        printf("Error: Could not open %s.tab\n", filename);
        return FILE_OPEN_ERROR;
    }

    // Read table header
    fread(&table_header, sizeof(table_file_header), 1, tab_file);
    num_columns = tpd->num_columns;
    col_entry = (cd_entry*)((char*)tpd + tpd->cd_offset);
    record_size = table_header.record_size;

    // Allocate buffer for the new record
    record_buffer = (char*)calloc(1, record_size);
    if (!record_buffer)
    {
        fclose(tab_file);
        return MEMORY_ERROR;
    }

    // Step 6: Parse and validate values
    record_ptr = record_buffer;
    for (int i = 0; i < num_columns; i++, col_entry++)
    {
        // Expect value (either int or string)

		printf("%s: ", col_entry->col_name);
        if (cur->tok_value == INT_LITERAL && col_entry->col_type == T_INT)
        {
            int int_value = atoi(cur->tok_string);	// Convert String to integer
            memcpy(record_ptr, &int_value, sizeof(int));
			printf("(int) %d ", *(int*)record_ptr);
			printf("\n");
            record_ptr += sizeof(int);
        }
        else if (cur->tok_value == STRING_LITERAL && col_entry->col_type == T_CHAR)
        {
            if (strlen(cur->tok_string) > col_entry->col_len)
            {
                printf("Error: String value too long for column %s.\n", col_entry->col_name);
                free(record_buffer);
                fclose(tab_file);
                return STRING_TOO_LONG;
            }
            memcpy(record_ptr, cur->tok_string, strlen(cur->tok_string));
			memset(record_ptr + strlen(cur->tok_string), 0, col_entry->col_len - (strlen(cur->tok_string)));
			printf(" (char*) %s ", (char*)record_ptr);
			printf("\n");
			record_ptr += col_entry->col_len;
        }
        else
        {
            // Type mismatch
            printf("Error: Type mismatch for column %s.\n", col_entry->col_name);
            free(record_buffer);
            fclose(tab_file);
            return TYPE_MISMATCH;
        }

        // Expect ',' or ')'
        cur = cur->next;
        if (i < num_columns - 1 && cur->tok_value != S_COMMA)
        {
            free(record_buffer);
            fclose(tab_file);
			perror(" comma ',' or right parenthesis ')' not found in token list\n");
            return INVALID_STATEMENT;
        }
        else if (i == num_columns - 1 && cur->tok_value != S_RIGHT_PAREN)
        {
            free(record_buffer);
            fclose(tab_file);
			perror("right parenthesis not found in token list\n");
            return INVALID_STATEMENT;
        }

        cur = cur->next;  // Move to the next token
    }

    // Step 7: Append the new record to the .tab file
    fseek(tab_file, table_header.file_size, SEEK_SET);  // Seek to end of file
    fwrite(record_buffer, record_size, 1, tab_file);

    // Update table header (increment record count and file size)
    table_header.num_records++;
    table_header.file_size += record_size;
    fseek(tab_file, 0, SEEK_SET);  // Rewind to the beginning of the file
    fwrite(&table_header, sizeof(table_file_header), 1, tab_file);

    // Step 8: Clean up and close file
    free(record_buffer);
    fclose(tab_file);
	
	printf("Inserted 1 Row.\n");

	// TODO: To be removed later. only use for debugging
	int rc = 0;
	cur = t_list;
	rc = print_tab_file(cur->tok_string);
	return rc;
}

int initialize_tpd_list(){
	int rc = 0;
	FILE *fhandle = NULL;
	//	struct _stat file_stat;
	struct stat file_stat;

	/* Open for read */
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
			if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				g_tpd_list = NULL;
				g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
				
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
			printf("dbfile.bin size = %d\n", file_stat.st_size);

			g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

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
	
int add_tpd_to_list(tpd_entry *tpd){
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
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

int drop_tpd_from_list(char *tabname){
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

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
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
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
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
							fwrite((char*)cur + cur->tpd_size,
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
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
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
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
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

tpd_entry* get_tpd_from_list(char *tabname){
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
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int create_tab_file(tpd_entry *tpd){
	FILE *tab_file;
	char filename[MAX_IDENT_LEN + 5];	// For "<table_name>.tab"
	int record_size = 0;
	table_file_header table_header;

	// Cconstruct filename for .tab file
	sprintf(filename, "%s.tab", tpd->table_name);

	// Open a new binary file
	tab_file = fopen(filename, "wb");
	if(!tab_file){
		return FILE_OPEN_ERROR;
	}

	// Calculate record size based on table's schema
	cd_entry *col_entry = (cd_entry*)((char*)tpd + tpd->cd_offset);
	for (int i=0;i<tpd->num_columns; i++, col_entry++){
		if (col_entry->col_type == T_INT){
			record_size += sizeof(int);	// 4 bytes for INT
		}
		else if(col_entry->col_type == T_CHAR){
			record_size += col_entry->col_len +1; // CHAR(n) with 1-byte length prefix
		}
		// TODO: Add support for other data types
	}

	// Round the record size to a multiple of 4 for fast disk IO
	if (record_size % 4 != 0){
		record_size += 4 - (record_size % 4);
	}

	// Initialize table file header
	memset(&table_header, 0, sizeof(table_file_header));
	table_header.file_size = sizeof(table_file_header);
	table_header.record_size = record_size;
	table_header.num_records =0;
	table_header.record_offset = sizeof(table_file_header);
	table_header.file_header_flag =0;
	table_header.tpd_ptr = tpd;

	// write header to .tab file
	fwrite(&table_header, sizeof(table_file_header), 1, tab_file);

	fclose(tab_file);
	return 0;
}

int print_tab_file(char *table_name){
    FILE *tab_file;
    char filename[MAX_IDENT_LEN + 5];  // For "<table_name>.tab"
    table_file_header table_header;
    tpd_entry *tpd;
    cd_entry *col_entry;
    char *record_buffer;
    int record_size, num_columns, num_records;
    
    // Construct the filename for the table's .tab file
    sprintf(filename, "%s.tab", table_name);
    
    // Open the .tab file for reading
    tab_file = fopen(filename, "rb");
    if (!tab_file)
    {
        // Handle error: Could not open file
        printf("Error: Could not open %s.tab\n", table_name);
        return FILE_OPEN_ERROR;
    }
    
    // Read the table file header
    fread(&table_header, sizeof(table_file_header), 1, tab_file);
    
    // Get the record size and number of records
    record_size = table_header.record_size;
    num_records = table_header.num_records;
    
    // Get the table descriptor from dbfile.bin
    tpd = get_tpd_from_list(table_name);  // Function to retrieve table descriptor
    if (!tpd)
    {
        printf("Error: Table descriptor not found in dbfile.bin\n");
        fclose(tab_file);
        return TABLE_NOT_EXIST;
    }

    num_columns = tpd->num_columns;
    col_entry = (cd_entry*)((char*)tpd + tpd->cd_offset);

    // Print column headers
	printf("\n");
    for (int i = 0; i < num_columns; i++, col_entry++)
    {
        printf("%s\t", col_entry->col_name);
    }
    printf("\n");
	printf("----------------------------------------------\n");
    
    // Allocate buffer for reading records
    record_buffer = (char*)malloc(record_size);
    if (!record_buffer)
    {
        // Handle error: Memory allocation failed
        printf("Error: Memory allocation failed\n");
        fclose(tab_file);
        return MEMORY_ERROR;
    }

    // Read and print each record
    for (int i = 0; i < num_records; i++)
    {
        fread(record_buffer, record_size, 1, tab_file);
        
        // Parse and print each field in the record
        col_entry = (cd_entry*)((char*)tpd + tpd->cd_offset);  // Reset column pointer
        char *record_ptr = record_buffer;
        for (int j = 0; j < num_columns; j++, col_entry++)
        {
            if (col_entry->col_type == T_INT)
            {
                int int_value;
                memcpy(&int_value, record_ptr, sizeof(int));
                printf("%d\t", int_value);
                record_ptr += sizeof(int);
            }
            else if (col_entry->col_type == T_CHAR)
            {
                char str_value[MAX_TOK_LEN + 1];
                memcpy(str_value, record_ptr, col_entry->col_len);
                str_value[col_entry->col_len] = '\0';  // Null-terminate the string
                printf("%s\t", str_value);
                record_ptr += col_entry->col_len + 1;  // Move past string and length prefix
            }
        }
        printf("\n");
    }

    // Clean up and close file
    free(record_buffer);
    fclose(tab_file);

    return 0;
}

int sem_select_query_handler(token_list *t_list){
	char table_name[MAX_IDENT_LEN];
    char *columns[MAX_NUM_COL];
    int *num_columns;

	char table_1[MAX_IDENT_LEN];
	char table_2[MAX_IDENT_LEN];
	char t1_join_column[MAX_IDENT_LEN];
	char t2_join_column[MAX_IDENT_LEN];
	char *t1_columns[MAX_NUM_COL];
	char *t2_columns[MAX_NUM_COL];
	int t1_num_columns;
	int t2_num_columns;

	int rc;

	int process_inner_join = 0;
	process_inner_join = has_inner_join(t_list);

	if (process_inner_join==0){
		// Parse the table and column names
		rc = parse_table_and_columns(t_list, table_name, columns, &num_columns);
		if (rc != 0) {
			printf("Error parsing table or columns\n");
			return rc;
		}

		// Execute the select operation
		rc = select_from_table(table_name, columns, *num_columns);
		if (rc != 0) {
			printf("Error executing select operation on table '%s'\n", table_name);
		}

		// Free allocated memory for column names
		for (int i = 0; i < *num_columns; i++) {
			free(columns[i]);
		}
	}
	else
	{

		rc = parse_inner_join_table_and_columns(t_list,
												table_1,
												table_2,
												t1_columns,
												t2_columns,
												t1_join_column,
												t2_join_column,
												&t1_num_columns,
												&t2_num_columns,
												columns,
												&num_columns);

		if (rc != 0) {
			printf("Error parsing table or columns\n");
			return rc;
		}
	}

    return rc;
}

int select_from_table(char* table_name, char** columns, int num_columns_to_select) {

	// Retrieve the table descriptor
    tpd_entry *tpd = get_tpd_from_list(table_name);
    if (!tpd) {
        fprintf(stderr, "Error: Table descriptor not found for table %s\n", table_name);
        return -1;
    }

    // Open the .tab file
    char filename[MAX_IDENT_LEN + 5];
    sprintf(filename, "%s.tab", table_name);
    FILE *tab_file = fopen(filename, "rb");
    if (!tab_file) {
        fprintf(stderr, "Error: Could not open table file %s\n", filename);
        return -1;
    }

    // Read the table header
    table_file_header table_header;
    fread(&table_header, sizeof(table_file_header), 1, tab_file);

    int num_columns = tpd->num_columns;
    cd_entry *col_entry = (cd_entry*)((char*)tpd + tpd->cd_offset);

    // Determine indices of selected columns
    int selected_col_indices[num_columns_to_select];
    for (int i = 0; i < num_columns_to_select; i++) {
        int found = 0;
        for (int j = 0; j < num_columns; j++) {
            if (strcmp(col_entry[j].col_name, columns[i]) == 0) {
                selected_col_indices[i] = j;
                found = 1;
                break;
            }
        }
        if (!found) {
            fprintf(stderr, "Error: Column %s not found in table %s\n", columns[i], table_name);
            fclose(tab_file);
            return -1;
        }
    }

	// Print separator line
	for (int c = 0; c < num_columns_to_select; c++) {
		printf("-----------------------"); // Adjust width to match column width
	}
	// Print headers
	printf("\n| ");
	for (int c = 0; c < num_columns_to_select; c++) {
		printf("%-20s | ", columns[c]);  // Adjust column width for alignment
	}
	printf("\n");

	// Print separator line
	for (int c = 0; c < num_columns_to_select; c++) {
		printf("-----------------------"); // Adjust width to match column width
	}
	printf("\n");

    // Read and print each record
    char *record_buffer = (char *)malloc(table_header.record_size);
    if (!record_buffer) {
        fprintf(stderr, "Memory allocation error\n");
        fclose(tab_file);
        return -1;
    }

    for (int i = 0; i < table_header.num_records; i++) {
        fread(record_buffer, table_header.record_size, 1, tab_file);

		printf("| ");

        // Print values of selected columns for the current record
		for (int j = 0; j < num_columns_to_select; j++) {
			int col_offset = 0;
			cd_entry *col = &col_entry[selected_col_indices[j]];

			// Calculate offset by summing the lengths of all preceding columns
			for (int k = 0; k < selected_col_indices[j]; k++) {
				col_offset += col_entry[k].col_len;
			}

			// Use col_offset to access the field in record_buffer
			char *field_ptr = record_buffer + col_offset;

			if (col->col_type == T_INT) {
				int int_value;
				memcpy(&int_value, field_ptr, sizeof(int));
				// printf("%d\t", int_value);
				printf("%-20d | ", int_value);
			} else if (col->col_type == T_CHAR) {
				char str_value[MAX_TOK_LEN + 1] = {0};
				strncpy(str_value, field_ptr, col->col_len);
				// printf("%s\t", str_value);
				printf("%-20s | ", str_value);
			}
		}
		printf("\n");
    }

	// Print separator line
	for (int c = 0; c < num_columns_to_select; c++) {
		printf("-----------------------"); // Adjust width to match column width
	}
	printf("\n");

    // Clean up
    free(record_buffer);
    fclose(tab_file);
    return 0;
}

int has_inner_join(token_list *tok_ptr){
	int k_inner_found = 0;
	while(tok_ptr && tok_ptr->tok_value != EOC){
		if (k_inner_found == 0){
			if (tok_ptr->tok_value == K_NATURAL){
				k_inner_found = 1;
			}
			tok_ptr = tok_ptr->next;
		}else{
			// Keyword INNER was found, now look for JOIN
			if (tok_ptr->tok_value != K_JOIN){
				perror("Invalid use of INNER keyword.");
				return INVALID_STATEMENT;
			}else{
				// JOIN keyword found
				return 1;
			}

		}
	}

	return 0;
}

int parse_table_and_columns(token_list *tok_ptr, char *table_name, char **columns, int **num_columns) {
    tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;

	int rc =0;

	// Step 1: Parse column names (until reaching "FROM")
	if (tok_ptr && tok_ptr -> tok_value == S_STAR){
		// Step 1 a: If Select ALL (*)

		// Check if next token is KEYWORD FROM
		tok_ptr = tok_ptr->next;
		if (tok_ptr && tok_ptr->tok_value != K_FROM){
			perror("Syntax error: Expected 'FROM' keyword");
			return INVALID_STATEMENT;
		}
		// Check if next token is IDENTIFIER <table_name>
		tok_ptr = tok_ptr->next;
		if (tok_ptr && tok_ptr->tok_value != IDENT){
			perror("Syntax error: Expected <table_name> Identifier after FROM keyword");
			return INVALID_STATEMENT;
		}

		// copy Table name in table_name pointer
		strncpy(table_name, tok_ptr->tok_string, MAX_IDENT_LEN);

		// Access table schema and fetch all columns
		tab_entry = get_tpd_from_list(table_name);

		if (!tab_entry){
			printf("Error: Table %s does not exist.\n", table_name);
			return TABLE_NOT_EXIST;
		}

		// Get list of columns from the schema
		*num_columns = &tab_entry->num_columns;

		int i=0;
		for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			i <= tab_entry->num_columns; i++, col_entry++)
		{
			// Access column names by col_entry->col_name
			columns[i] = (char *)malloc(MAX_IDENT_LEN);
			if (!columns[i]) {
				perror("Memory allocation failed for column name");
				return MEMORY_ERROR;
			}
			strncpy(columns[i], col_entry->col_name, MAX_IDENT_LEN);
		}

	}else{
		int col_count=0;
		// Step 1 b: If Select column
		while (tok_ptr && tok_ptr->tok_value != K_FROM) {
			if (tok_ptr->tok_value == IDENT) {
				// Allocate space for column name and copy it
				columns[col_count] = (char *)malloc(MAX_IDENT_LEN);
				if (!columns[col_count]) {
					perror("Memory allocation failed for column name");
					return MEMORY_ERROR;
				}
				strncpy(columns[col_count], tok_ptr->tok_string, MAX_IDENT_LEN);
				col_count++;
			}

			// Move to the next token (expect a comma or "FROM")
			tok_ptr = tok_ptr->next;
			if (tok_ptr && tok_ptr->tok_value == S_COMMA) {
				tok_ptr = tok_ptr->next;  // Skip comma
			}
		}

		
		if (col_count == 0){
			perror("Expected * operator or list of columns after SELECT\n");
			return INVALID_STATEMENT;
		}

		*num_columns = &col_count;

		// Step 2: Parse the "FROM" keyword
		if (!tok_ptr || tok_ptr->tok_value != K_FROM) {
			perror("Syntax error: Expected 'FROM' keyword");
			return INVALID_STATEMENT;
		}
		tok_ptr = tok_ptr->next;

		// Step 3: Parse table name
		if (!tok_ptr || tok_ptr->tok_value != IDENT) {
			perror("Syntax error: Expected table name after 'FROM'");
			return INVALID_STATEMENT;
		}
		strncpy(table_name, tok_ptr->tok_string, MAX_IDENT_LEN);
	}

    return 0;
}

int parse_inner_join_table_and_columns(token_list *tok_ptr,
										char *table_1,
										char *table_2,
										char **table_1_columns,
										char **table_2_columns,
										char *table_1_join_col,
										char *table_2_join_col,
										int *num_columns_table_1,
										int *num_columns_table_2,
										char **columns,
										int **num_columns ){
	
	// Step 0: Count Number of columns
	token_list *temp_ptr = tok_ptr;
	int column_count = 0;
	while (tok_ptr && tok_ptr->tok_value != K_FROM){
			if (tok_ptr->tok_value == IDENT) {
				column_count++;
			}

			// Move to the next token (expect a comma or "FROM")
			tok_ptr = tok_ptr->next;
			if (tok_ptr && tok_ptr->tok_value == S_COMMA) {
				tok_ptr = tok_ptr->next;  // Skip comma
			}
	}
	// Check FROM keyword
	if (!tok_ptr || tok_ptr->tok_value != K_FROM) {
		perror("**Syntax error: Expected 'FROM' keyword");
		return INVALID_STATEMENT;
	}

	// Reset tok_ptr to the start
	tok_ptr = temp_ptr;
	*num_columns = &column_count;

	int i=0;
	// Step 1: Extract columns
	while (tok_ptr && tok_ptr->tok_value != K_FROM) {
			if (tok_ptr->tok_value == IDENT) {
				// Allocate space for column name and copy it
				columns[i] = (char *)malloc(MAX_IDENT_LEN);
				if (!columns[i]) {
					perror("Memory allocation failed for column name");
					return MEMORY_ERROR;
				}
				
				strncpy(columns[i], tok_ptr->tok_string, MAX_IDENT_LEN);
				i++;
			}

			// Move to the next token (expect a comma or "FROM")
			tok_ptr = tok_ptr->next;
			if (tok_ptr && tok_ptr->tok_value == S_COMMA) {
				tok_ptr = tok_ptr->next;  // Skip comma
			}
	}

	// Step 3: Parse table_1 name
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != IDENT) {
		perror("Syntax error: Expected table name after 'FROM'");
		return INVALID_STATEMENT;
	}
	strncpy(table_1, tok_ptr->tok_string, MAX_IDENT_LEN);

	// Step 4: Parse Inner Join clause
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != K_NATURAL){
		perror("Syntax error: Expected INNER keyword after 'FROM'");
		return INVALID_STATEMENT;
	}
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != K_JOIN){
		perror("Syntax error: Expected JOIN keyword after 'INNER'");
		return INVALID_STATEMENT;
	}

	// Step 5: Parse table_2 name
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != IDENT) {
		perror("Syntax error: Expected table name after 'FROM'");
		return INVALID_STATEMENT;
	}
	strncpy(table_2, tok_ptr->tok_string, MAX_IDENT_LEN);

	// Step 6: Parse ON keyword
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != K_ON){
		perror("Syntax error: Expected ON keyword after table_1 of INNER JOIN");
		return INVALID_STATEMENT;
	}

	// Step 7: Parse table_1.column
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != IDENT) {
		perror("Syntax error: Expected column name after table name");
		return INVALID_STATEMENT;
	}
	strncpy(table_1_join_col, tok_ptr->tok_string, MAX_IDENT_LEN);

	// Step 8: Parse '=' symbol
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != S_EQUAL){
		perror("Syntax error: Expected '=' symbol after columna name in INNER JOIN");
		return INVALID_STATEMENT;
	}

	// Step 9: Parse table_2.column
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != IDENT) {
		perror("Syntax error: Expected column name after '=' symbol in INNER JOIN");
		return INVALID_STATEMENT;
	}
	strncpy(table_2_join_col, tok_ptr->tok_string, MAX_IDENT_LEN);

	// Step 10: Parse EOC
	tok_ptr = tok_ptr->next;
	if (!tok_ptr || tok_ptr->tok_value != EOC){
		perror("Syntax error: Expected end of query");
		return INVALID_STATEMENT;
	}

	// Fetch table descriptors for both tables
    tpd_entry *tpd1 = get_tpd_from_list(table_1);
    tpd_entry *tpd2 = get_tpd_from_list(table_2);

    if (!tpd1 || !tpd2) {
        fprintf(stderr, "Error: One or both tables do not exist.\n");
        return TABLE_NOT_EXIST;
    }

    // Locate join columns in both tables
    int join_col_offset_table1 = -1, join_col_offset_table2 = -1;
    cd_entry *col_entry_1 = (cd_entry*)((char*)tpd1 + tpd1->cd_offset);
    cd_entry *col_entry_2 = (cd_entry*)((char*)tpd2 + tpd2->cd_offset);
    cd_entry *join_col_entry_1 = NULL, *join_col_entry_2 = NULL;

    for (int i = 0; i < tpd1->num_columns; i++) {
        if (strcmp(col_entry_1[i].col_name, table_1_join_col) == 0) {
            join_col_offset_table1 = i;
            join_col_entry_1 = &col_entry_1[i];
            break;
        }
    }
    for (int i = 0; i < tpd2->num_columns; i++) {
        if (strcmp(col_entry_2[i].col_name, table_2_join_col) == 0) {
            join_col_offset_table2 = i;
            join_col_entry_2 = &col_entry_2[i];
            break;
        }
    }

    if (!join_col_entry_1 || !join_col_entry_2) {
        fprintf(stderr, "Error: Join columns not found in tables.\n");
        return COLUMN_NOT_EXIST;
    }

    // Open files for both tables
    char filename1[MAX_IDENT_LEN + 5], filename2[MAX_IDENT_LEN + 5];
    sprintf(filename1, "%s.tab", table_1);
    sprintf(filename2, "%s.tab", table_2);

    FILE *file1 = fopen(filename1, "rb"), *file2 = fopen(filename2, "rb");
    if (!file1 || !file2) {
        fprintf(stderr, "Error: Could not open table files.\n");
        return FILE_OPEN_ERROR;
    }

    // Read headers and set up record buffers
    table_file_header header1, header2;
    fread(&header1, sizeof(table_file_header), 1, file1);
    fread(&header2, sizeof(table_file_header), 1, file2);

    char *record_buffer_1 = (char*)malloc(header1.record_size);
    char *record_buffer_2 = (char*)malloc(header2.record_size);
    if (!record_buffer_1 || !record_buffer_2) {
        fprintf(stderr, "Memory allocation error.\n");
        return MEMORY_ERROR;
    }

	// Print separator line
	for (int c = 0; c < **num_columns; c++) {
		printf("-----------------------"); // Adjust width to match column width
	}
	// Print headers
	printf("\n| ");
	for (int c = 0; c < **num_columns; c++) {
		printf("%-20s | ", columns[c]);  // Adjust column width for alignment
	}
	printf("\n");

	// Print separator line
	for (int c = 0; c < **num_columns; c++) {
		printf("-----------------------"); // Adjust width to match column width
	}
	printf("\n");

    // Iterate over records in table_1
    for (int i = 0; i < header1.num_records; i++) {
        fread(record_buffer_1, header1.record_size, 1, file1);

        // Get join column value from table_1
        void *join_col_value1 = NULL;
		// Step 1: Calculate offset for join column in table_1
		int offset = 0;
		for (int k = 0; k < join_col_offset_table1; k++) {
			offset += col_entry_1[k].col_len;  // Summing the lengths of preceding columns
		}
		char *join_col_ptr1 = record_buffer_1 + offset;
        if (join_col_entry_1->col_type == T_INT) {
			// Allocate memory for integer type and copy value
			join_col_value1 = malloc(sizeof(int));
			if (join_col_value1) {
				memcpy(join_col_value1, join_col_ptr1, sizeof(int));  // `join_col_value1` should now hold `2001`
			}
		} else if (join_col_entry_1->col_type == T_CHAR) {
			// Allocate memory for char array type and copy value
			join_col_value1 = malloc(join_col_entry_1->col_len + 1);  // +1 for null-termination
			if (join_col_value1) {
				strncpy((char *)join_col_value1, join_col_ptr1, join_col_entry_1->col_len);
				((char *)join_col_value1)[join_col_entry_1->col_len] = '\0';  // Ensure null-terminated string
			}
		}

        // Iterate over records in table_2
        fseek(file2, sizeof(table_file_header), SEEK_SET);  // Reset to start of table_2
        for (int j = 0; j < header2.num_records; j++) {
            fread(record_buffer_2, header2.record_size, 1, file2);

            // Get join column value from table_2
            void *join_col_value2 = NULL;

			// Step 1: Calculate offset for join column in table_2
			int offset2 = 0;
			for (int k = 0; k < join_col_offset_table2; k++) {
				offset2 += col_entry_2[k].col_len;  // Summing the lengths of preceding columns
			}

			// Step 2: Access join column in record_buffer_2 using calculated offset
			char *join_col_ptr2 = record_buffer_2 + offset2;

			if (join_col_entry_2->col_type == T_INT) {
				// Allocate memory for integer type and copy value
				join_col_value2 = malloc(sizeof(int));
				if (join_col_value2) {
					memcpy(join_col_value2, join_col_ptr2, sizeof(int));  // `join_col_value2` should now hold the correct integer value
				}
			} else if (join_col_entry_2->col_type == T_CHAR) {
				// Allocate memory for char array type and copy value
				join_col_value2 = malloc(join_col_entry_2->col_len + 1);  // +1 for null-termination
				if (join_col_value2) {
					strncpy((char *)join_col_value2, join_col_ptr2, join_col_entry_2->col_len);
					((char *)join_col_value2)[join_col_entry_2->col_len] = '\0';  // Ensure null-terminated string
				}
			}

            // Compare join column values and print if they match
			if (((join_col_entry_1->col_type == T_INT && join_col_entry_2->col_type == T_INT) && *(int*)join_col_value1 == *(int*)join_col_value2) ||
				((join_col_entry_1->col_type == T_CHAR && join_col_entry_2->col_type == T_CHAR) && strcmp((char*)join_col_value1, (char*)join_col_value2) == 0)) {
				
				printf("| ");
				// Print matched columns from both tables based on the `columns` array
				for (int c = 0; c < **num_columns; c++) {
					int col_offset = 0;
					void *value_ptr = NULL;
					cd_entry *col_entry = NULL;
					int is_table1_column = 0;

					// Check if the column is in table_1 or table_2
					for (int k = 0; k < tpd1->num_columns; k++) {
						// printf("here4\n");
						if (strcmp(columns[c], col_entry_1[k].col_name) == 0) {
							is_table1_column = 1;
							col_entry = &col_entry_1[k];
							
							// Calculate offset in record_buffer_1 for this column
							for (int l = 0; l < k; l++) {
								col_offset += col_entry_1[l].col_len;
							}
							break;
						}
					}
					if (!is_table1_column) {
						// Column is from table_2
						for (int k = 0; k < tpd2->num_columns; k++) {
							if (strcmp(columns[c], col_entry_2[k].col_name) == 0) {
								col_entry = &col_entry_2[k];
								
								// Calculate offset in record_buffer_2 for this column
								for (int l = 0; l < k; l++) {
									col_offset += col_entry_2[l].col_len;
								}
								break;
							}
						}
					}

					if (col_entry) {
						// Determine the value pointer based on column location (table_1 or table_2)
						if (is_table1_column) {
							value_ptr = record_buffer_1 + col_offset;
						} else {
							value_ptr = record_buffer_2 + col_offset;
						}

						// Print the value based on column type
						if (col_entry->col_type == T_INT) {
							int int_value;
							memcpy(&int_value, value_ptr, sizeof(int));
							printf("%-20d | ", int_value);
							// printf("%d\t", int_value);
						} else if (col_entry->col_type == T_CHAR) {
							char char_value[MAX_IDENT_LEN + 1] = {0};
							strncpy(char_value, (char*)value_ptr, col_entry->col_len);
							char_value[col_entry->col_len] = '\0'; // Ensure null-termination for safe printing
							// printf("%s\t", char_value);
							printf("%-20s | ", char_value);
						}
					}
				}
				printf("\n"); // Move to the next line after printing each joined row
			}
        }
    }

	// Print separator line
	for (int c = 0; c < **num_columns; c++) {
		printf("-----------------------"); // Adjust width to match column width
	}
	printf("\n");

    // Cleanup
    free(record_buffer_1);
    free(record_buffer_2);
    fclose(file1);
    fclose(file2);

    return 0;

}

int select_inner_join_tables(char *table_1,
								char *table_2,
								char** t1_columns,
								char** t2_columns,
								int t1_num_columns,
								int t2_num_columns){
	/*
		Step 1: Read table_1 and table_2
		Step 2: Map rows by matching columns
		Step 3: print mapped rows
	 */
	return 0;
}
