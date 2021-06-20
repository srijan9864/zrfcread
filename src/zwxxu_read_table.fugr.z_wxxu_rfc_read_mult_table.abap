FUNCTION z_wxxu_rfc_read_mult_table.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IM_TABLE_PARAMS) TYPE  ZWTXXU_TABLE_PARAMS
*"     VALUE(IM_JOIN_COND) TYPE  ZWTXXU_TABLE_JOIN OPTIONAL
*"     VALUE(IM_DISTINCT) TYPE  BOOLEAN OPTIONAL
*"  EXPORTING
*"     VALUE(EX_MESSAGES) TYPE  ZWTXXU_MESSAGES
*"     VALUE(EX_COUNT) TYPE  I
*"----------------------------------------------------------------------

*******************RFC READ Table with JOIN*****************************
*                                                                      *
* Program       : Z_WXXU_RFC_READ_MULT_TABLE                           *
* Description   : This RFC function module helps in giving count of    *
*                 table entries based on dynamic where condition & join*
*                 condition                                            *
* Input         : Where condition, Join Condition & Distinct Flag      *
* Output        : Count of Rows & Messages                             *
* Creation date : 1st Jan, 2021                                        *
* Author        : Srijan Sarmah (srijan9864)                           *
*----------------------------------------------------------------------*

******** Types Declaration ***********

  TYPES: BEGIN OF l_ty_data,             "For Join table
           tab_comb   TYPE string,
           tabname1   TYPE tabname,
           fieldname1 TYPE fieldname,
           tabname2   TYPE tabname,
           fieldname2 TYPE fieldname,
         END OF l_ty_data,
         BEGIN OF l_ty_tabname,        "For Tablename
           tabname TYPE tabname,
         END OF l_ty_tabname,
         BEGIN OF l_ty_alias,         "For Alias
           tabname TYPE tabname,
           alias   TYPE i,
         END OF l_ty_alias,
         BEGIN OF l_ty_aliasdone,
           tabname TYPE tabname,
           exists  TYPE boolean,
         END OF l_ty_aliasdone.

*************** Data Declaration ***************************

  DATA: l_it_where_clause TYPE rsds_where_tab,                                               "Where clause with range
        l_v_where_clause2 TYPE string,                                                       "Where clause String
        l_v_from          TYPE string,                                                       "From String
        l_it_join         TYPE STANDARD TABLE OF l_ty_data,                                  "Join Table
        l_it_fields       TYPE  STANDARD TABLE OF zwxx_cl_rfc_validation_check=>ty_fields   "Filter table for fields
                            WITH NON-UNIQUE SORTED KEY exi COMPONENTS exists,
        l_it_filter       TYPE zwxx_cl_rfc_validation_check=>t_ty_fields,                   "Table to store fields
        l_v_message       TYPE string,                                                       "Message String
        l_it_tableinvalid TYPE STANDARD TABLE OF l_ty_tabname,                               "Table exists table
        l_v_old           TYPE string,                                                       "Loop bypasser
        l_it_alias        TYPE STANDARD TABLE OF l_ty_alias,                                 "Store Alias
        l_v_alias         TYPE string,                                                       "Alias
        l_it_rangewhere   TYPE STANDARD TABLE OF rsds_range,                                 "Where with range
        l_it_where        TYPE STANDARD TABLE OF rsds_where,                                 "Where to pass in FM
        l_it_aliasdone    TYPE STANDARD TABLE OF l_ty_aliasdone.                             "If alias in JOIN is already done

**************** Constants Declaration *********************

  CONSTANTS: l_c_fname  TYPE funcname VALUE 'Z_WXXU_RFC_READ_MULT_TABLE',
             l_c_fgroup TYPE rfc_name VALUE 'ZWXXU_READ_TABLE'.

  CALL FUNCTION 'AUTHORITY_CHECK_RFC'
    EXPORTING
      userid             = sy-uname
      functiongroup      = l_c_fgroup
      functionname       = l_c_fname
    EXCEPTIONS
      function_not_exist = 1
      user_dont_exist    = 2
      rfc_no_authority   = 3
      OTHERS             = 4.

  IF sy-subrc IS INITIAL.

* Loop at input parameteres to derive the where condition
    LOOP AT im_table_params ASSIGNING FIELD-SYMBOL(<l_fs_params>).


      DATA(l_v_tabix) = sy-tabix.

      CLEAR: l_it_where_clause[].

* Alias is stored as counter
      APPEND VALUE #( tabname = <l_fs_params>-tabname alias = l_v_tabix ) TO l_it_alias.

* Fields of a particular table
      l_it_fields = VALUE #( FOR l_wa_fields IN <l_fs_params>-input_filter (
                                                tabname = <l_fs_params>-tabname
                                                fieldname = l_wa_fields-fieldname ) ).


* Validate table and its fields
      zwxx_cl_rfc_validation_check=>validate_table_fields(
         EXPORTING
           im_tablename    = <l_fs_params>-tabname
         IMPORTING
           ex_table_exists = DATA(l_v_table_exists)
         CHANGING
           im_fields       = l_it_fields ).

      IF l_v_table_exists EQ abap_true.

        IF l_it_fields IS NOT INITIAL.
*  The fields that doesn't exists
          APPEND LINES OF FILTER #( l_it_fields USING KEY exi WHERE exists = abap_false ) TO l_it_filter.

          IF  line_exists( l_it_fields[ exists = abap_false ] ).
            DATA(l_v_incorrect) = abap_true.
          ELSE.

            l_v_alias = l_it_alias[ tabname = <l_fs_params>-tabname ]-alias. "Alias

            APPEND VALUE #( tablename = CONV tabname( l_v_alias )
                            frange_t  = <l_fs_params>-input_filter ) TO l_it_rangewhere.


* Derive the where condition table
            CALL FUNCTION 'FREE_SELECTIONS_RANGE_2_WHERE'
              EXPORTING
                field_ranges  = l_it_rangewhere
              IMPORTING
                where_clauses = l_it_where.

            l_it_where_clause = l_it_where[ tablename = CONV tabname( l_v_alias ) ]-where_tab.

* Concatenate the where clause from internal table
            IF l_it_where_clause[] IS NOT INITIAL.
              IF l_v_tabix NE 1 AND l_v_where_clause2 IS NOT INITIAL.
                l_v_where_clause2 = |{ l_v_where_clause2 } AND|.
              ENDIF.
              LOOP AT l_it_where_clause ASSIGNING FIELD-SYMBOL(<l_fs_where_clause>).
                SHIFT <l_fs_where_clause>-line LEFT DELETING LEADING space.
                IF l_v_where_clause2 IS INITIAL.
                  l_v_where_clause2 = |{ <l_fs_where_clause>-line }|.
                ELSE.
                  l_v_where_clause2 = |{ l_v_where_clause2 } { <l_fs_where_clause>-line }|.
                ENDIF.
              ENDLOOP.
* Replace the field names with alias~fieldname
              LOOP AT l_it_fields ASSIGNING FIELD-SYMBOL(<l_fs_fields>).
                REPLACE ALL OCCURRENCES OF <l_fs_fields>-fieldname IN l_v_where_clause2 WITH |{ CONV tabname( l_v_alias ) }~{ <l_fs_fields>-fieldname }|.
              ENDLOOP.
            ENDIF.
          ENDIF.
        ENDIF.
      ELSE.
*   Tables that doesn's exits
        APPEND VALUE #( tabname = <l_fs_params>-tabname ) TO l_it_tableinvalid.
      ENDIF.
      CLEAR : l_it_fields, l_v_table_exists, l_v_alias.
    ENDLOOP.

    IF l_v_incorrect EQ abap_false AND l_it_tableinvalid IS INITIAL .

* Preparing the FROM string when
      IF im_join_cond IS INITIAL.
        TRY.
            l_v_from = |{ im_table_params[ 1 ]-tabname } AS { l_it_alias[ tabname = im_table_params[ 1 ]-tabname ]-alias }|.
          CATCH cx_sy_itab_line_not_found.
            CLEAR l_v_from.
        ENDTRY.

      ELSE.

        l_it_join = VALUE #( FOR l_wa_join IN im_join_cond ( tabname1   = l_wa_join-tabname1
                                                             tabname2   = l_wa_join-tabname2
                                                             fieldname1 = l_wa_join-fieldname1
                                                             fieldname2 = l_wa_join-fieldname2
                                                             tab_comb   = l_wa_join-tabname1 &&
                                                                          l_wa_join-tabname2 ) ).
        SORT l_it_join BY tab_comb.

        CLEAR l_v_old.

        SORT l_it_alias BY alias ASCENDING.

        LOOP AT l_it_join ASSIGNING FIELD-SYMBOL(<l_fs_join>).


          DATA(l_v_index) = sy-tabix.

** Validate the join table and fields

          APPEND VALUE #( tabname = <l_fs_join>-tabname1
                          fieldname = <l_fs_join>-fieldname1
                         ) TO l_it_fields.
          DATA(l_it_field1) = l_it_fields.
          CLEAR l_it_fields.
          APPEND VALUE #( tabname = <l_fs_join>-tabname2
                  fieldname = <l_fs_join>-fieldname2
                 ) TO l_it_fields.
          DATA(l_it_field2) = l_it_fields.
          CLEAR l_it_fields.
          zwxx_cl_rfc_validation_check=>validate_table_fields(
            EXPORTING
              im_tablename    = <l_fs_join>-tabname1
            IMPORTING
              ex_table_exists = DATA(l_v_tab1exist)
            CHANGING
              im_fields       = l_it_field1 ).


          zwxx_cl_rfc_validation_check=>validate_table_fields(
            EXPORTING
              im_tablename    = <l_fs_join>-tabname2
            IMPORTING
              ex_table_exists = DATA(l_v_tab2exist)
            CHANGING
              im_fields       = l_it_field2 ).
          IF l_v_tab1exist EQ abap_true AND l_v_tab2exist EQ abap_true
            AND line_exists( l_it_field1[ exists = abap_true ] )
            AND line_exists( l_it_field2[ exists = abap_true ] ) .
** Check if for the validated table alias exists in the alias table, if not assign new one.

            IF NOT line_exists( l_it_alias[ tabname = <l_fs_join>-tabname1 ] ).

              APPEND VALUE #( tabname = <l_fs_join>-tabname1  alias = l_it_alias[ lines( l_it_alias ) ]-alias + 1 ) TO l_it_alias.

            ENDIF.

            IF NOT line_exists( l_it_alias[ tabname = <l_fs_join>-tabname2 ] ).

              APPEND VALUE #( tabname = <l_fs_join>-tabname2  alias = l_it_alias[ lines( l_it_alias ) ]-alias + 1 ) TO l_it_alias.

            ENDIF.

* Build the FROM string
            IF l_v_old NE <l_fs_join>-tab_comb.
              TRY.
                  IF l_v_index EQ 1.
                    l_v_from = l_v_from && |{ <l_fs_join>-tabname1 } AS { l_it_alias[ tabname = <l_fs_join>-tabname1 ]-alias }| &&
                                           | INNER JOIN { <l_fs_join>-tabname2 } AS { l_it_alias[ tabname = <l_fs_join>-tabname2 ]-alias }| &&
                                           | ON { l_it_alias[ tabname = <l_fs_join>-tabname1 ]-alias }~{ <l_fs_join>-fieldname1 }| &&
                                           | = { l_it_alias[ tabname = <l_fs_join>-tabname2 ]-alias }~{ <l_fs_join>-fieldname2 }|.

                    IF NOT line_exists( l_it_aliasdone[ tabname = <l_fs_join>-tabname1 exists = abap_true ] ).
                      APPEND VALUE #( tabname = <l_fs_join>-tabname1 exists = abap_true ) TO l_it_aliasdone.
                    ENDIF.

                    IF NOT line_exists( l_it_aliasdone[ tabname = <l_fs_join>-tabname2 exists = abap_true ] ).
                      APPEND VALUE #( tabname = <l_fs_join>-tabname2 exists = abap_true ) TO l_it_aliasdone.
                    ENDIF.
                  ELSEIF l_v_index NE 1 AND line_exists( l_it_aliasdone[ tabname = <l_fs_join>-tabname2 exists = abap_true ] ).
                    l_v_from = l_v_from && | INNER JOIN { <l_fs_join>-tabname2 }| &&
                                        | ON { l_it_alias[ tabname = <l_fs_join>-tabname1 ]-alias }~{ <l_fs_join>-fieldname1 }| &&
                                        | = { l_it_alias[ tabname = <l_fs_join>-tabname2 ]-alias }~{ <l_fs_join>-fieldname2 }|.
                  ELSE.
                    l_v_from = l_v_from && | INNER JOIN { <l_fs_join>-tabname2 } AS { l_it_alias[ tabname = <l_fs_join>-tabname2 ]-alias }| &&
                                        | ON { l_it_alias[ tabname = <l_fs_join>-tabname1 ]-alias }~{ <l_fs_join>-fieldname1 }| &&
                                        | = { l_it_alias[ tabname = <l_fs_join>-tabname2 ]-alias }~{ <l_fs_join>-fieldname2 }|.

                    IF NOT line_exists( l_it_aliasdone[ tabname = <l_fs_join>-tabname1 exists = abap_true ] ).
                      APPEND VALUE #( tabname = <l_fs_join>-tabname1 exists = abap_true ) TO l_it_aliasdone.
                    ENDIF.

                    IF NOT line_exists( l_it_aliasdone[ tabname = <l_fs_join>-tabname2 exists = abap_true ] ).
                      APPEND VALUE #( tabname = <l_fs_join>-tabname2 exists = abap_true ) TO l_it_aliasdone.
                    ENDIF.
                  ENDIF.
                CATCH cx_sy_itab_line_not_found.
                  CLEAR l_v_from.
                  l_v_incorrect = abap_true.
              ENDTRY.
              l_v_old = <l_fs_join>-tab_comb.
              CONTINUE.
            ENDIF.

            TRY.
                l_v_from = l_v_from && | AND { l_it_alias[ tabname = <l_fs_join>-tabname1 ]-alias }~{ <l_fs_join>-fieldname1 }| &&
                                 | = { l_it_alias[ tabname = <l_fs_join>-tabname2 ]-alias }~{ <l_fs_join>-fieldname2 }|.
              CATCH cx_sy_itab_line_not_found.
                l_v_incorrect = abap_true.
                CLEAR l_v_from.
            ENDTRY.
          ELSE.
*  IF table 1 exists but the field is invalid
            IF l_v_tab1exist EQ abap_true AND line_exists( l_it_field1[ exists = abap_false ] ).

              CLEAR l_v_message.

              CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                EXPORTING
                  msgid               = 'ZWGL_CLASS'
                  msgnr               = '065'
                  msgv1               = <l_fs_join>-tabname1
                  msgv2               = <l_fs_join>-fieldname1
                IMPORTING
                  message_text_output = l_v_message.


              APPEND VALUE #( msgty = sy-abcde+4(1)
                              msgid = 'ZWGL_CLASS'
                              msgno = '065'
                              msgtext = l_v_message ) TO ex_messages.

* If table1 doesn't exists
            ELSEIF l_v_tab1exist EQ abap_false.
              CLEAR l_v_message.
              CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                EXPORTING
                  msgid               = 'ZWGL_CLASS'
                  msgnr               = '067'
                  msgv1               = <l_fs_join>-tabname1
                  msgv2               = sy-sysid
                IMPORTING
                  message_text_output = l_v_message.


              APPEND VALUE #( msgty = sy-abcde+4(1)
                              msgid = 'ZWGL_CLASS'
                              msgno = '067'
                              msgtext = l_v_message ) TO ex_messages.

            ENDIF.
*  IF table 2 exists but the field is invalid
            IF l_v_tab2exist EQ abap_true AND line_exists( l_it_field2[ exists = abap_false ] ).

              CLEAR l_v_message.

              CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                EXPORTING
                  msgid               = 'ZWGL_CLASS'
                  msgnr               = '065'
                  msgv1               = <l_fs_join>-tabname2
                  msgv2               = <l_fs_join>-fieldname2
                IMPORTING
                  message_text_output = l_v_message.


              APPEND VALUE #( msgty = sy-abcde+4(1)
                              msgid = 'ZWGL_CLASS'
                              msgno = '065'
                              msgtext = l_v_message ) TO ex_messages.

* If table2 doesn't exists
            ELSEIF l_v_tab2exist EQ abap_false.
              CLEAR l_v_message.
              CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                EXPORTING
                  msgid               = 'ZWGL_CLASS'
                  msgnr               = '067'
                  msgv1               = <l_fs_join>-tabname2
                  msgv2               = sy-sysid
                IMPORTING
                  message_text_output = l_v_message.


              APPEND VALUE #( msgty = sy-abcde+4(1)
                              msgid = 'ZWGL_CLASS'
                              msgno = '067'
                              msgtext = l_v_message ) TO ex_messages.

            ENDIF.
            l_v_incorrect = abap_true.
          ENDIF.
        ENDLOOP.

      ENDIF.

      IF l_v_from IS NOT INITIAL AND l_v_incorrect EQ abap_false.
* Select without distinct
        IF im_distinct EQ abap_false.
          TRY.
* Select with where condition
              IF l_v_where_clause2 IS NOT INITIAL.
                SELECT COUNT(*)
                  FROM (l_v_from)
                  INTO @ex_count
                  WHERE (l_v_where_clause2).
              ELSE.
* Select without where conditon
                SELECT COUNT(*)
                  FROM (l_v_from)
                  INTO @ex_count.
              ENDIF.
              IF sy-subrc IS INITIAL.
*  Build successful message
                DATA(l_v_countstr) = CONV symsgv( ex_count ).
                CONDENSE l_v_countstr.
                CLEAR l_v_message.
                CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                  EXPORTING
                    msgid               = 'ZWGL_CLASS'
                    msgnr               = '066'
                    msgv1               = l_v_countstr
                  IMPORTING
                    message_text_output = l_v_message.
                CLEAR l_v_countstr.

                APPEND VALUE #( msgty = 'S'
                    msgid = 'ZWGL_CLASS'
                    msgno = '066'
                    msgtext = l_v_message ) TO ex_messages.
              ELSE.
*  Build unsuccessful message
                CLEAR l_v_message.
                DATA(l_v_system) = CONV symsgv( sy-sysid && sy-mandt ).
                CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                  EXPORTING
                    msgid               = 'ZWGL_CLASS'
                    msgnr               = '068'
                    msgv1               = l_v_system
                  IMPORTING
                    message_text_output = l_v_message.
                CLEAR l_v_countstr.

                APPEND VALUE #( msgty = 'W'
                    msgid = 'ZWGL_CLASS'
                    msgno = '068'
                    msgtext = l_v_message ) TO ex_messages.


              ENDIF.
* To catch other exceptions that might occur for dynamic select query (such as invalid sign/option)
            CATCH cx_root INTO DATA(l_v_o_root).

              APPEND VALUE #( msgty = sy-abcde+4(1)
                    msgid = sy-msgid
                    msgno = sy-msgno
                    msgtext = l_v_o_root->get_text( ) ) TO ex_messages.

              CLEAR l_v_countstr.
          ENDTRY.
        ELSE.                  "Select with distinct
          TRY.
              IF l_v_where_clause2 IS NOT INITIAL.
*    In presence of where clause
                SELECT DISTINCT COUNT(*)
                  FROM (l_v_from)
                  INTO @ex_count
                  WHERE (l_v_where_clause2).
              ELSE.
*    Where clause absent
                SELECT DISTINCT COUNT(*)
                  FROM (l_v_from)
                  INTO @ex_count.
              ENDIF.
              IF sy-subrc IS INITIAL.
*   Build successful message
                l_v_countstr = CONV symsgv( ex_count ).
                CONDENSE l_v_countstr.
                CLEAR l_v_message.
                CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                  EXPORTING
                    msgid               = 'ZWGL_CLASS'
                    msgnr               = '066'
                    msgv1               = l_v_countstr
                  IMPORTING
                    message_text_output = l_v_message.

                CLEAR l_v_countstr.

                APPEND VALUE #( msgty = 'S'
                    msgid = 'ZWGL_CLASS'
                    msgno = '066'
                    msgtext = l_v_message ) TO ex_messages.
              ELSE.
*   Build unsuccesful message
                CLEAR l_v_message.
                l_v_system = CONV symsgv( sy-sysid && sy-mandt ).
                CALL FUNCTION 'MESSAGE_TEXT_BUILD'
                  EXPORTING
                    msgid               = 'ZWGL_CLASS'
                    msgnr               = '068'
                    msgv1               = l_v_system
                  IMPORTING
                    message_text_output = l_v_message.
                CLEAR l_v_countstr.

                APPEND VALUE #( msgty = 'W'
                    msgid = 'ZWGL_CLASS'
                    msgno = '068'
                    msgtext = l_v_message ) TO ex_messages.
              ENDIF.
* To catch other exceptions that might occur for dynamic select query (such as invalid sign/option)
            CATCH  cx_root INTO l_v_o_root.
              APPEND VALUE #( msgty = sy-abcde+4(1)
                   msgid = sy-msgid
                   msgno = sy-msgno
                   msgtext = l_v_o_root->get_text( ) ) TO ex_messages.
              CLEAR l_v_countstr.
          ENDTRY.
        ENDIF.
      ENDIF.
    ELSEIF l_it_tableinvalid IS NOT INITIAL.
* Build message tab for table invalid
      LOOP AT l_it_tableinvalid ASSIGNING FIELD-SYMBOL(<l_fs_wrongtab>).
        CLEAR l_v_message.

        CALL FUNCTION 'MESSAGE_TEXT_BUILD'
          EXPORTING
            msgid               = 'ZWGL_CLASS'
            msgnr               = '067'
            msgv1               = <l_fs_wrongtab>-tabname
            msgv2               = sy-sysid
          IMPORTING
            message_text_output = l_v_message.


        APPEND VALUE #( msgty = sy-abcde+4(1)
                        msgid = 'ZWGL_CLASS'
                        msgno = '067'
                        msgtext = l_v_message ) TO ex_messages.


      ENDLOOP.

    ELSEIF l_v_incorrect EQ abap_true.
* Build message for invalid table-field combination
      LOOP AT l_it_filter ASSIGNING FIELD-SYMBOL(<l_fs_filt>).

        CLEAR l_v_message.

        CALL FUNCTION 'MESSAGE_TEXT_BUILD'
          EXPORTING
            msgid               = 'ZWGL_CLASS'
            msgnr               = '065'
            msgv1               = <l_fs_filt>-fieldname
            msgv2               = <l_fs_filt>-tabname
          IMPORTING
            message_text_output = l_v_message.


        APPEND VALUE #( msgty = sy-abcde+4(1)
                        msgid = 'ZWGL_CLASS'
                        msgno = '065'
                        msgtext = l_v_message ) TO ex_messages.


      ENDLOOP.

    ENDIF.

  ELSEIF sy-subrc = 3.

    CLEAR l_v_message.

    CALL FUNCTION 'MESSAGE_TEXT_BUILD'
      EXPORTING
        msgid               = 'ZWGL_CLASS'
        msgnr               = '069'
        msgv1               = sy-uname
        msgv2               = l_c_fname
      IMPORTING
        message_text_output = l_v_message.


    APPEND VALUE #( msgty = sy-abcde+4(1)
                    msgid = 'ZWGL_CLASS'
                    msgno = '069'
                    msgtext = l_v_message ) TO ex_messages.

  ELSE.

    CLEAR l_v_message.

    CALL FUNCTION 'MESSAGE_TEXT_BUILD'
      EXPORTING
        msgid               = sy-msgid
        msgnr               = sy-msgno
        msgv1               = sy-msgv1
        msgv2               = sy-msgv2
      IMPORTING
        message_text_output = l_v_message.


    APPEND VALUE #( msgty = sy-msgty
                    msgid = sy-msgid
                    msgno = sy-msgno
                    msgtext = l_v_message ) TO ex_messages.

  ENDIF.

ENDFUNCTION.
