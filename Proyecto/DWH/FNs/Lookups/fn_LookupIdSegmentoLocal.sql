use MX_DWH

if object_id ('etl.fn_LookupIdSegmentoLocal', 'FN') is not null
    drop function etl.fn_LookupIdSegmentoLocal;
Go

create function etl.fn_LookupIdSegmentoLocal (
		@codigo varchar(255)
)
returns numeric(10)
as
BEGIN

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 23/02/2022
	-- Description:	LookUp Id Segmento Local
	-- =============================================
	-- 23/02/2022 - Version Inicial (INC9045398)

	declare @rtn numeric(10)

	if @codigo is null or ltrim(rtrim(@codigo)) = ''
		select @rtn = max(id_segmento_local)
		from dbo.DCTE_LK_SEGMENTO_LOCAL b
		where cod_segmento_local in ('NA')
	else
		select @rtn = max(id_segmento_local)
		from dbo.DCTE_LK_SEGMENTO_LOCAL b
		where cod_segmento_local = @codigo

	return isnull(@rtn,-1)

END
GO
-- select etl.fn_LookupIdSegmentoLocal(null)
