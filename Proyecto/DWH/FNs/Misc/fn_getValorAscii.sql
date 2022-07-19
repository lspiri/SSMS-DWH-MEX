use MX_DWH
Go

if object_id ('etl.fn_getValorAscii', 'FN') is not null
	drop function etl.fn_getValorAscii
GO

create function etl.fn_getValorAscii (@pValor varchar(100))
	returns numeric
BEGIN
	
	declare @rtn numeric(15), @contar int
	
	set @contar = 1
	set @rtn = 0
	while @contar <= len(@pValor)
	 begin
		set @rtn = round(@rtn + ascii(substring(@pValor, @contar, 1)) / (pi()/@contar),0)
		set @contar = @contar + 1
	 end
		
	return @rtn							
	
END
GO
-- select etl.fn_getValorAscii('dasdas')