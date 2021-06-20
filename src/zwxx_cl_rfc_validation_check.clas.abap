class ZWXX_CL_RFC_VALIDATION_CHECK definition
  public
  final
  create public .

public section.

  types:
    BEGIN OF ty_fields,
      tabname   TYPE tabname,
      fieldname TYPE fieldname,
      exists    TYPE boolean,
    END OF ty_fields .
  types:
    T_TY_FIELDS TYPE STANDARD TABLE OF ty_fields .

  class-methods VALIDATE_TABLE_FIELDS
    importing
      !IM_TABLENAME type TABNAME
      !IM_MSGTY type SYMSGTY optional
    exporting
      !EX_TABLE_EXISTS type BOOLEAN
    changing
      !IM_FIELDS type T_TY_FIELDS optional .
protected section.
private section.
ENDCLASS.



CLASS ZWXX_CL_RFC_VALIDATION_CHECK IMPLEMENTATION.


  METHOD validate_table_fields.
*********************VALIDATE_TABLE_FIELDS******************************
*                                                                      *
* Method       : VALIDATE_TABLE_FIELDS                                 *
* Description   : To validate the table and its fields                 *
* Input         : Table and its fields                                 *
* Output        : Validated table and its fields                       *
* Creation date : 12th March, 2021                                     *
* Author        : Srijan Sarmah (srijan9864)                           *
*----------------------------------------------------------------------*
    DATA: l_r_fieldname TYPE RANGE OF fieldname.

    IF im_tablename IS NOT INITIAL.

      SELECT @abap_true
        FROM dd03l
        UP TO 1 ROWS
        INTO @DATA(l_v_boolean)
        WHERE tabname EQ @im_tablename.
      ENDSELECT.
      IF sy-subrc IS INITIAL.
        ex_table_exists = l_v_boolean.

        IF im_fields IS NOT INITIAL.

          l_r_fieldname = VALUE #( FOR l_wa_fields IN im_fields (
                                                sign = 'I'
                                                option = 'EQ'
                                                low = l_wa_fields-fieldname
                                                high = '' ) ).

          SELECT tabname,
                 fieldname
            FROM dd03l
            INTO TABLE @DATA(l_it_tabfields)
            WHERE tabname EQ @im_tablename
            AND   fieldname IN @l_r_fieldname
            AND   as4local EQ 'A'.

          IF sy-subrc IS INITIAL.

            LOOP AT im_fields ASSIGNING FIELD-SYMBOL(<l_fs_fields>).

              <l_fs_fields>-exists = COND #( WHEN line_exists( l_it_tabfields[ fieldname = <l_fs_fields>-fieldname ] )
                                             THEN abap_true
                                             ELSE abap_false ).
              <l_fs_fields>-tabname = im_tablename.

            ENDLOOP.

          ENDIF.

          IF line_exists( im_fields[ exists = abap_false ] ) AND im_msgty IS NOT INITIAL.
*     For message type, how the message will be displayed
            MESSAGE e064(zwgl_class) WITH im_tablename DISPLAY LIKE im_msgty.
          ENDIF.

        ENDIF.
      ELSEIF sy-subrc IS NOT INITIAL AND im_msgty IS NOT INITIAL.
        MESSAGE e067(zwgl_class) WITH im_tablename sy-sysid DISPLAY LIKE im_msgty.
      ENDIF.
    ENDIF.

  ENDMETHOD.
ENDCLASS.
