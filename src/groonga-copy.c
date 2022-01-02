#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <groonga.h>
#include <time.h>

int
main(int argc, char **argv)
{
  grn_ctx from_ctx_, to_ctx_;
  grn_ctx *from_ctx = &from_ctx_, *to_ctx = &to_ctx_;
  grn_obj *from_db = NULL, *from_table = NULL, *from_column = NULL;
  grn_obj *to_db = NULL, *to_table = NULL, *to_column = NULL;
  grn_bool is_reference = GRN_FALSE;
  grn_bool is_weight_float = GRN_FALSE;
  grn_bool is_time = GRN_FALSE;
  grn_obj *ref_table = NULL;
  grn_obj to_buf;
  
  const char *from_path = NULL;
  const char *from_table_name = NULL;
  const char *from_column_name = NULL;
  const char *to_path = NULL;
  const char *to_table_name = NULL;
  const char *to_column_name = NULL;
  bool is_add = false;
  
  if(argc != 7 && argc != 8){
    fprintf(stderr, "input: from_db_path from_table from_column to_db_path to_table to_column is_add\n");
    return -1;
  }
  setvbuf(stdout, (char *)NULL, _IONBF, 0);
  from_path = argv[1];
  from_table_name = argv[2];
  from_column_name = argv[3];
  to_path = argv[4];
  to_table_name = argv[5];
  to_column_name = argv[6];
  if (argc == 8) {
    is_add = true;
  }

  if (grn_init()) {
    fprintf(stderr, "grn_init() failed\n");
    return -1;
  }

  if (grn_ctx_init(from_ctx, 0) || grn_ctx_init(to_ctx, 0)) {
    fprintf(stderr, "grn_ctx_init() failed\n");
    return -1;
  }

  from_db = grn_db_open(from_ctx, from_path);
  if (!from_db) {
    fprintf(stderr, "from_db initialize failed\n");
    goto exit;
  }
  to_db = grn_db_open(to_ctx, to_path);
  if (!to_db) {
    fprintf(stderr, "to_db initialize failed\n");
    goto exit;
  }

  from_table = grn_ctx_get(from_ctx, from_table_name, strlen(from_table_name));
  if (!from_table) {
    fprintf(stderr, "not found from table\n");
    goto exit;
  }
  from_column = grn_obj_column(from_ctx, from_table,
                               from_column_name,
                               strlen(from_column_name));
  if (!from_column) {
    fprintf(stderr, "not found from table\n");
    goto exit;
  }

  is_reference = grn_obj_is_reference_column(from_ctx, from_column);

  if (is_reference) {
    int flags = 0;
    ref_table = grn_ctx_at(from_ctx, grn_obj_get_range(from_ctx, from_column));
    if (grn_obj_is_vector_column(from_ctx, from_column)) {
      flags = GRN_OBJ_VECTOR;
    }
    /*
    if (ref_table) {
      GRN_RECORD_INIT(&to_buf, flags, grn_obj_id(from_ctx, ref_table));
    } else {
      GRN_TEXT_INIT(&to_buf, flags);
    }
    */
    GRN_TEXT_INIT(&to_buf, flags);
    if (grn_obj_is_weight_vector_column(from_ctx, from_column)) {
      to_buf.header.flags |= GRN_OBJ_WITH_WEIGHT;
      grn_column_flags col_flags = grn_column_get_flags(from_ctx, from_column);
      if (col_flags & GRN_OBJ_WEIGHT_FLOAT32) {
        is_weight_float = GRN_TRUE;
        to_buf.header.flags |= GRN_OBJ_WEIGHT_FLOAT32;
      }
    }
    if (ref_table) {
      if (ref_table->header.domain == GRN_DB_TIME) {
        is_time = GRN_TRUE;
      }
    }
  }

  to_table = grn_ctx_get(to_ctx, to_table_name, strlen(to_table_name));
  if (!to_table) {
    fprintf(stderr, "not found to table\n");
    goto exit;
  }
  to_column = grn_obj_column(to_ctx, to_table,
                             to_column_name,
                             strlen(to_column_name));
  if (!to_column) {
    fprintf(stderr, "not found to table\n");
    goto exit;
  }

  {
    grn_table_cursor *cursor;
    grn_id from_id;
    grn_obj from_value;
    cursor = grn_table_cursor_open(from_ctx, from_table,
                                   NULL, 0,
                                   NULL, 0,
                                   0, -1, GRN_CURSOR_ASCENDING);
    if (!cursor) {
      fprintf(stderr, "failed open cursor\n");
      goto exit;
    }
    GRN_VOID_INIT(&from_value);
    while ((from_id = grn_table_cursor_next(from_ctx, cursor)) != GRN_ID_NIL) {
      void *key;
      int key_size;
      grn_id to_id;

      key_size = grn_table_cursor_get_key(from_ctx, cursor, &key);

      GRN_BULK_REWIND(&from_value);
      grn_obj_get_value(from_ctx, from_column, from_id, &from_value);

      if (is_add) {
        to_id = grn_table_add(to_ctx, to_table, key, key_size, NULL);
      } else {
        to_id = grn_table_get(to_ctx, to_table, key, key_size);
      }
      if (to_id % 100000 == 0) {
        printf("from_id(%u)->to_id(%u)\n", from_id, to_id);
      }
      if (to_id != GRN_ID_NIL) {
        if (is_reference) {
          switch (from_value.header.type) {
          case GRN_BULK :
            {
              grn_id id;
              char key_name[GRN_TABLE_MAX_KEY_SIZE];
              int key_len;

              id = GRN_RECORD_VALUE(&from_value);
              key_len = grn_table_get_key(from_ctx, ref_table, id, key_name, GRN_TABLE_MAX_KEY_SIZE);
              if (is_time) {
                struct tm tm;
                char formatted_time_buffer[4096];
                size_t formatted_time_size;

                if (grn_time_to_tm(to_ctx, *(int64_t *)&key_name, &tm)) {
                  formatted_time_size = strftime(formatted_time_buffer,
                                                 sizeof(formatted_time_buffer),
                                                 "%Y-%m-%d 00:00:00",
                                                 &tm);
	          GRN_TEXT_SET(to_ctx, &to_buf, formatted_time_buffer, formatted_time_size);
                }
              } else {
	        /*
                grn_id nid;
                nid = grn_table_get(from_ctx, ref_table, key_name, key_len);
                GRN_BULK_REWIND(&to_buf);
                GRN_RECORD_SET(to_ctx, &to_buf, nid);
	        */
	        GRN_TEXT_SET(to_ctx, &to_buf, key_name, key_len);
	      }
              grn_obj_set_value(to_ctx, to_column, to_id, &to_buf, GRN_OBJ_SET);
            }
            break;
          case GRN_UVECTOR :
            {
              int i, n_elements;
              unsigned int weight;
	      float weight_float;

              GRN_BULK_REWIND(&to_buf);
              n_elements = grn_vector_size(from_ctx, &from_value);
              for (i = 0; i < n_elements; i++) {
                grn_id id;
                if (is_weight_float) {
                  id = grn_uvector_get_element_record(from_ctx, &from_value, i, &weight_float);
                } else {
                  id = grn_uvector_get_element(from_ctx, &from_value, i, &weight);
                }
                {
                  char key_name[GRN_TABLE_MAX_KEY_SIZE];
                  int key_len;
                  key_len = grn_table_get_key(from_ctx, ref_table, id, key_name, GRN_TABLE_MAX_KEY_SIZE);
                  if (is_time) {
                    struct tm tm;
                    char formatted_time_buffer[4096];
                    size_t formatted_time_size;

                    if (grn_time_to_tm(to_ctx, *(int64_t *)&key_name, &tm)) {
                      formatted_time_size = strftime(formatted_time_buffer,
                                                     sizeof(formatted_time_buffer),
						     "%Y-%m-%d 00:00:00",
                                                     &tm);
                      if (is_weight_float) {
                        grn_vector_add_element_float(to_ctx, &to_buf, formatted_time_buffer, formatted_time_size, weight_float, GRN_DB_TEXT);
                      } else {
                        grn_vector_add_element(to_ctx, &to_buf, formatted_time_buffer, formatted_time_size, weight, GRN_DB_TEXT);
                      }
		    }
                  } else {
                    if (is_weight_float) {
                      grn_vector_add_element_float(to_ctx, &to_buf, key_name, key_len, weight_float, GRN_DB_TEXT);
                    } else {
                      grn_vector_add_element(to_ctx, &to_buf, key_name, key_len, weight, GRN_DB_TEXT);
                    }
                  }
                  /*
                  grn_id nid;
                  nid = grn_table_get(from_ctx, ref_table, key_name, key_len);
                  if (nid) {
                    if (is_weight_float) {
                      grn_uvector_add_element_record(to_ctx, &to_buf, nid, weight_float);
                    } else {
                      grn_uvector_add_element(to_ctx, &to_buf, nid, weight);
                    }
                  }
                  */
                }
              }
              if (n_elements > 0) {
                grn_obj_set_value(to_ctx, to_column, to_id, &to_buf, GRN_OBJ_SET);
              }
            }
            break;
          default :
            break;
          }
        } else {
          grn_obj_set_value(to_ctx, to_column, to_id, &from_value, GRN_OBJ_SET);
        }
      }
    }
    GRN_OBJ_FIN(from_ctx, &from_value);
    grn_table_cursor_close(from_ctx, cursor);
  }

exit :
  if (is_reference) {
    GRN_OBJ_FIN(to_ctx, &to_buf);
    if (ref_table) {
      grn_obj_unlink(from_ctx, ref_table);
    }
  }

  if (from_column) {
    if (grn_obj_close(from_ctx, from_column)) {
      fprintf(stderr, "grn_obj_close() failed\n");
      return -1;
    }
  }
  if (from_table) {
    if (grn_obj_close(from_ctx, from_table)) {
      fprintf(stderr, "grn_obj_close() failed\n");
      return -1;
    }
  }

  if (to_column) {
    if (grn_obj_close(to_ctx, to_column)) {
      fprintf(stderr, "grn_obj_close() failed\n");
      return -1;
    }
  }
  if (to_table) {
    if (grn_obj_close(to_ctx, to_table)) {
      fprintf(stderr, "grn_obj_close() failed\n");
      return -1;
    }
  }

  if (from_db) {
    if (grn_obj_close(from_ctx, from_db)) {
      fprintf(stderr, "grn_obj_close() failed\n");
      return -1;
    }
  }
  if (to_db) {
    if (grn_obj_close(to_ctx, to_db)) {
      fprintf(stderr, "grn_obj_close() failed\n");
      return -1;
    }
  }

  if (grn_ctx_fin(from_ctx) || grn_ctx_fin(to_ctx)) {
    fprintf(stderr, "grn_ctx_fin() failed\n");
    return -1;
  }

  if (grn_fin()) {
    fprintf(stderr, "grn_fin() failed\n");
    return -1;
  }
  return 0;
}
