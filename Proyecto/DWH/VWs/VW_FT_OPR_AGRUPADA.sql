Use MX_DWH
Go

if OBJECT_ID('VW_FT_OPR_AGRUPADA', 'V') is not null
	drop view VW_FT_OPR_AGRUPADA
Go

CREATE VIEW VW_FT_OPR_AGRUPADA /*WITH SCHEMABINDING*/ AS
SELECT ft.ID_MES,
    ft.ID_CC_OPR,
    ft.ID_AGRUPADOR,
    ft.ID_EMPRESA,
    ft.ID_LINEA,
    ft.ID_CUENTA,
    ft.ID_TIPO_SERV_DETALLE,
	ft.ORIGEN,
	round(ft.REAL,2) as REAL,
    round(ft.BUDGET,2) as BUDGET,
	ft.REAL_NUM,
	ft.REAL_DEN,
	ft.BUDGET_NUM,
	ft.BUDGET_DEN,
	ft.DIVISION,
	lk.ID_ZONA
from dbo.FT_OPR_AGRUPADA ft,
	dbo.VW_LK_CC_OPR_HIST lk
where ft.id_mes = lk.id_mes
	and ft.id_cc_opr = lk.id_cc
Go
