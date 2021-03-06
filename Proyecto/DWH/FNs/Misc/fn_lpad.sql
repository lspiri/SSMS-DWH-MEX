use MX_DWH
go

if object_id ('etl.fn_lpad', 'FN') is not null
    drop function etl.fn_lpad;
Go

create function etl.fn_lpad (
    @string varchar(max), 
    @length INT,          
    @pad CHAR             
)
	returns varchar(max)
as
begin
    return replicate(@pad, @length - len(@string)) + @string;
END
Go
-- select etl.fn_lpad('132', 8, '0')
