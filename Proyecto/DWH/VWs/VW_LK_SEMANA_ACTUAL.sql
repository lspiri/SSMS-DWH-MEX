use MX_DWH
go

if object_id('VW_LK_SEMANA_ACTUAL', 'V') is not null
	drop view VW_LK_SEMANA_ACTUAL
go

create view VW_LK_SEMANA_ACTUAL AS
select id_Semana, id_semana_ant 
from DTPO_LK_SEMANAS 
where etl.fn_truncFecha ( getdate () ) between semana_Desde and Semana_Hasta
go