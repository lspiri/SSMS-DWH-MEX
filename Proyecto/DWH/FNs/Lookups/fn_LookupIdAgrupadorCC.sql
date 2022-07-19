use MX_DWH

if object_id ('etl.fn_LookupIdAgrupadorCC', 'FN') is not null
    drop function etl.fn_LookupIdAgrupadorCC;
Go

create function etl.fn_LookupIdAgrupadorCC (
		@codigo varchar(30)
)
returns numeric(10)
as
BEGIN

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 21/03/2022
	-- Description:	LookUp Id Agrupador CC
	-- =============================================
	-- 21/03/2022 - Version Inicial

	declare @rtn numeric(10)

	if @codigo is null or ltrim(rtrim(@codigo)) = ''
		select @rtn = max(id_agrupador)
		from dbo.DGEN_LK_AGRUPADOR_CC b
		where desc_agrupador in ('NA', 'Not Available')
	else
		select @rtn = max(id_agrupador)
		from dbo.DGEN_LK_AGRUPADOR_CC b
		where cod_agrupador = @codigo

	return isnull(@rtn,-1)
END
GO
-- select etl.fn_LookupIdAgrupadorCC('NA')