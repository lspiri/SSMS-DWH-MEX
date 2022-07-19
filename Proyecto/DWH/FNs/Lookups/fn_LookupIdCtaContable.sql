use MX_DWH

if object_id ('etl.fn_LookupIdCtaContable', 'FN') is not null
    drop function etl.fn_LookupIdCtaContable;
Go

create function etl.fn_LookupIdCtaContable (
		@codigo varchar(255)
)
returns numeric(10)
as
BEGIN

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 31/03/2022
	-- Description:	LookUp Id Cuenta Contable
	-- =============================================
	-- 31/03/2022 - Version Inicial

	declare @rtn numeric(10)

	if @codigo is null or ltrim(rtrim(@codigo)) = ''
		select @rtn = max(id_cuenta)
		from dbo.DGEN_LK_CTA_CONTABLE b
		where cod_cuenta in ('NA')
	else
		select @rtn = max(id_cuenta)
		from dbo.DGEN_LK_CTA_CONTABLE b
		where cod_cuenta = @codigo

	return isnull(@rtn,-1)

END
GO
-- select etl.fn_LookupIdCtaContable(null)
