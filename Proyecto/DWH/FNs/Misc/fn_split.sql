use MX_DWH
go

if object_id ('etl.fn_split', 'TF') is not null
    drop function etl.fn_split;
Go

create function etl.fn_split (
	@lista varchar(1000), @sep char(1))
	returns @listaValores  table (valor varchar(50))
as
begin	

	declare @pos_ini int, @pos_sig int, @valor varchar(50)	

	
	set @pos_sig = charindex (@sep, @lista, 1)
	if @pos_sig = 0
		begin
			set @valor = ltrim(rtrim( @lista ))
			if isnull(@valor,'') <> ''
				insert into @listaValores values ( @valor )
		end
	else
		set @pos_ini = 1 
		while isnull(@pos_sig,0) > 0 --and @pos_sig <= len(@lista)
		  begin
			if @pos_sig - 1 > 0
			  begin
				set @valor = ltrim(rtrim(substring(@lista, @pos_ini, @pos_sig - @pos_ini )))
				if isnull(@valor,'') <> ''
					insert into @listaValores values ( @valor )
			  end	
			
			-- next
			set @pos_ini = @pos_sig + 1
			set @pos_sig = charindex (@sep , @lista, @pos_ini)
			if @pos_sig = 0 and @pos_ini <= len(@lista)
				set @pos_sig = len(@lista) + 1
		 end
 
	return
end
GO
-- select * from etl.fn_split ('3,4,5,6,97,9', ',') as lista