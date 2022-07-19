-- DROP VIEW INT_ARG.v_mx_cliente
CREATE OR REPLACE FORCE VIEW INT_ARG.V_MX_CLIENTE AS
SELECT cod_cliente,
   cuit,
   razon_social,
   domicilio,
   provincia,
   localidad,
   cod_postal,
   telefono,
   cant_pi,
   creacion,
   cod_segmento_local,
   segmentacion,
   segmento_mercado,
   cast('MX' as varchar2(2)) as cod_pais,
   cast('A' as varchar2(1)) as estado,
   cast('0' as varchar2(30)) as nro_cliente,
   cast('0' as varchar2(40)) as prospecto,
   cast(null as varchar2(150)) as cod_actividad, 
   cast(null as varchar2(150)) as cod_tipo_cliente,
   cast(null as varchar2(240)) as dom_numero, 
   cast(null as varchar2(240)) as dom_piso,
   guid
FROM (SELECT trim(cl.id) as cod_cliente,
         cl.cuit,
         cl.razon_social,
         cl.domicilio,
         jur.nombre                as provincia,
         billing_city              as localidad,
         billing_postalcode        as cod_postal,
         case when trim (phone) = '0' then null else phone end as telefono,
         cl.cantidad_rrhh          as cant_pi,
         cl.segmentation           as segmentacion_sf,
         cl.creation_date          as creacion,
         lpad (sl.codigo, 10, '0') as cod_segmento_local,
         nvl(segmentation,'NA')    as segmentacion,
         substr(market_segment,1,1) as segmento_mercado,
         r.guid
      from cliente@primex            cl,
        ref_jurisdiccion@primex   jur,
        ref_segmento_local@primex sl,
        intlat.rel_id_cuenta@priarg r
      where 1=1
        and cl.ref_jurisdiccion_id = jur.id(+)
        and cl.ref_segmento_local_id = sl.id(+)
        and cl.id = r.cuenta_id (+)
        and 'MX' = r.cod_pais (+) 
      );
/      

CREATE OR REPLACE SYNONYM USER_DW_EBS.V_MX_CLIENTE FOR INT_ARG.V_MX_CLIENTE;
/

-- select * from ref_jurisdiccion@primex
-- select * from V_MX_CLIENTE