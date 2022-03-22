use MX_DWH

if object_id ('etl.fn_LookupIdSegmentoGlobal', 'FN') is not null
    drop function etl.fn_LookupIdSegmentoGlobal;
Go

create function etl.fn_LookupIdSegmentoGlobal (
		@codigo varchar(255)
)
returns numeric(10)
as
BEGIN

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 23/02/2022
	-- Description:	LookUp Id Segmento Global
	-- =============================================
	-- 23/02/2022 - Version Inicial

	declare @rtn numeric(10)

	if @codigo is null or ltrim(rtrim(@codigo)) = ''
		select @rtn = max(id_segmento_global)
		from dbo.DCTE_LK_SEGMENTO_GLOBAL b
		where cod_segmento_global in ('NA')
	else
		select @rtn = max(id_segmento_global)
		from dbo.DCTE_LK_SEGMENTO_GLOBAL b
		where cod_segmento_global = @codigo

	return isnull(@rtn,0)
END
GO
-- select etl.fn_LookupIdSegmentoGlobal('SME')
