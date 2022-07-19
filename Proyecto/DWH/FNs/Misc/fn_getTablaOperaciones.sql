use MX_DWH

if object_id ('etl.fn_getTablaOperaciones', 'TF') is not null
    drop function etl.fn_getTablaOperaciones;
Go

create function etl.fn_getTablaOperaciones (
	@calculo varchar(1000))
	returns @operaciones  table (nro int,
								valor varchar(50),
								tipo char(3) )
as
begin

	-- =============================================
	-- Author:		Luciano Silvera
	-- Create date: 07/04/2022
	-- Description:	Obtiene Operaciones de Calculo
	-- =============================================
	-- 19/04/2022 - Version Inicial

	declare @pos int, 
		@caracter varchar(1), @valor varchar(50),
		@cont int

	set @calculo = replace(@calculo, ' ', '')
	set @cont = 0
	set @pos = 1
	set @valor = ''

	while @pos > 0 and @pos <= len(@calculo) 
	begin
		set @caracter = substring(@calculo, @pos, 1)
		if (isNumeric(@caracter) = 1 and @caracter not in ('+', '-', '/', '*')) or upper(@caracter) = 'F'
			begin
				set @valor = @valor + @caracter
			end
		else
			begin
				set @cont = @cont + 1
				insert into @operaciones values (@cont, @valor, 'NUM') 

				set @cont = @cont + 1
				insert into @operaciones values (@cont, @caracter, 'OPE') 

				set @valor = ''
			end

		-- next
		set @pos = @pos + 1
	end

	if @valor != '' and ((isNumeric(@valor) = 1 and @caracter not in ('+', '-', '/', '*')) or upper(@valor) like 'F%')
		begin
			set @cont = @cont + 1
			insert into @operaciones values (@cont, @valor, 'NUM') 
		end

	return
end
go
-- select * from etl.fn_getTablaOperaciones ('100024+100025+100026') as oprs
-- select * from etl.fn_getTablaOperaciones ('2500518/f365*2500519') as oprs
-- select * from etl.fn_getTablaOperaciones ('f48') as oprs
-- select * from etl.fn_getTablaOperaciones ('F0 - 1900471') as oprs




