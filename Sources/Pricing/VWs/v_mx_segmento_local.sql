-- DROP VIEW INT_ARG.v_mx_segmento_local;
CREATE OR REPLACE FORCE VIEW INT_ARG.V_MX_SEGMENTO_LOCAL
(
   CODIGO,
   NOMBRE
)
AS
   SELECT LPAD (codigo, 10, '0') AS codigo, nombre
     FROM ref_segmento_local@primex
    WHERE UPPER (TRIM (nombre)) NOT IN ('NA', 'N/A', 'NOT AVAILABLE')
   ORDER BY 1;


CREATE OR REPLACE SYNONYM USER_DW_EBS.V_MX_SEGMENTO_LOCAL FOR INT_ARG.V_MX_SEGMENTO_LOCAL;

-- select * from v_mx_segmento_local