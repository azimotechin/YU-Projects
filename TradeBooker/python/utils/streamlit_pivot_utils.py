import streamlit as st
import pandas as pd
from typing import Callable, Optional, Dict, Any

import utils.env_utils as envu
logger = envu.get_logger()


def get_aggrid_status_panel(show_rowcount=True, show_aggregates=True):
    panels = []
    if show_rowcount:
        panels.append({"statusPanel": "agTotalAndFilteredRowCountComponent", "align": "left"})
    if show_aggregates:
        panels.append({"statusPanel": "agAggregationComponent", "statusPanelParams": {"aggFuncs": ["sum", "avg", "count"]}})

    return { "statusPanels": panels }

def get_js_conditional_formatting(absValue, colorLow='#F8D7DA', colorHigh='#ADD8E6', colorNone='#FFFFFF'):
    import st_aggrid
    
    style = '{ "font-weight": "normal", "backgroundColor": "color" }'
    style_none = '{ "font-weight": "normal", "backgroundColor": "color" }'
    
    return st_aggrid.JsCode(f"""
        function(params) {{
            let color = params.value > {absValue} ? '{colorHigh}' : params.value < -{absValue} ? '{colorLow}' : '{colorNone}';
            return color == '{colorNone}' ? {style_none} : {style};
        }}
    """)

def create_grid_builder(df, minWidth=100, maxWidth=None, width=None, headerName="Group", showSidebar=True, pivotDefaultExpanded=0, pivotMode=True, headerMenuFilterOnly=True, suppressRowCount=True, onFirstDataRendered=None, pinned='left', show_status_bar=True):
    from st_aggrid import GridOptionsBuilder

    gb = GridOptionsBuilder.from_dataframe(df)
    statusPanels = get_aggrid_status_panel()

    gb.configure_grid_options(
        pivotMode=pivotMode, minWidth=minWidth, maxWidth=maxWidth, width=width, grandTotalRow="bottom", sidebar=showSidebar, hiddenByDefault=True, suppressAggFuncInHeader=True, rowHeight=20, groupDefaultExpanded=pivotDefaultExpanded, enableAdvancedFilter=False, pivotColumnGroupTotals="before", mainMenuItems=["filterMenuTab"], enableRangeSelection=True, onFirstDataRendered=onFirstDataRendered, enableStatusBar=show_status_bar, statusBar=statusPanels if show_status_bar else None,
        autoGroupColumnDef=dict(autoSizeStrategy=None, filter="agGroupColumnFilter", headerName=headerName, minWidth=minWidth, width=width, maxWidth=maxWidth, pinned='left',
                                cellRendererParams=dict(suppressCount=suppressRowCount)))

    if showSidebar:
        gb.configure_side_bar(filters_panel=True, columns_panel=True, defaultToolPanel='')

    gb.configure_default_column(resizable=True, filterable=False, sortable=True, editable=False,
        filter="agMultiColumnFilter",
        filterParams=dict(excelMode='default'),
        menuTabs=["filterMenuTab"] if headerMenuFilterOnly else None,
    )

    return gb

def get_js_numberformat(fractionDigits=0, zeroString='0', zeroCompareValue=0.01, highValueDigits=None, highValue=100, postFix='', divideBy=1, positive_indicator='', agg_levels_to_show=[], round_large_numbers=False, always_show_if_text_matches=None):
    import st_aggrid

    largeFractionDigits = fractionDigits if highValueDigits is None else highValueDigits
    postValueText = '' if divideBy == 1 else f' / {divideBy}'

    agg_levels_to_show_js = '[' + ', '.join(f'"{level}"' for level in agg_levels_to_show) + ']'
    if len(agg_levels_to_show_js) == 0:
        agg_levels_to_show_js = ''

    always_show_text = '' if always_show_if_text_matches is None else always_show_if_text_matches

    return st_aggrid.JsCode(f"""
        function customFormatter(params) {{
            // Check if the row is a group row and the group field is in the filter list
            if ({agg_levels_to_show} != '' && !{agg_levels_to_show_js}.includes(params.node.field)) {{
                if ('{always_show_text}' != '' && params.node.key != null && params.node.key.includes('{always_show_text}')) {{
                    // Continue
                }}
                else
                {{
                    return '';
                }}
            }}

            var abs_value = Math.abs(params.value);
            if (isNaN(abs_value))
                return params.value;

            if (params.value == null || abs_value < {zeroCompareValue}) {{
                return '{zeroString}';
            }}

            var value = params.value;

            if ('{round_large_numbers}' == 'True') {{
                if (abs_value > 2000) {{
                    abs_value = Math.round(params.value / 100) * 100;
                    value = Math.abs(abs_value);
                }} else if (abs_value > 100) {{
                    abs_value = Math.round(params.value / 10) * 10;
                    value = Math.abs(abs_value);
                }}
            }}

            return (value > 0 ?
                ( '{positive_indicator}' + Number(value).toLocaleString(undefined, {{
                    minimumFractionDigits: abs_value < {highValue} ? {fractionDigits} : {largeFractionDigits},
                    maximumFractionDigits: abs_value < {highValue} ? {fractionDigits} : {largeFractionDigits}
                }}) + '{postFix}{postValueText}' ) :
                ( Number(value).toLocaleString(undefined, {{
                    minimumFractionDigits: abs_value < {highValue} ? {fractionDigits} : {largeFractionDigits},
                    maximumFractionDigits: abs_value < {highValue} ? {fractionDigits} : {largeFractionDigits}
                }}) + '{postFix}{postValueText}' )
            );
        }}
    """)

class PivotBuilder:
    def __init__(
        self,
        control_key: str,
        df: pd.DataFrame,
        gridbuilder_create_callback: Callable[['PivotBuilder', str, pd.DataFrame], Any] = None,
        gridbuilder_ready_callback: Callable[['PivotBuilder', str, Any, pd.DataFrame], None] = None,
        columns_ready_callback: Callable[['PivotBuilder', str, Dict[str, Dict[str, Any]]], None] = None
    ):
        from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
        self.control_key = control_key
        self.df = df
        self.gridbuilder_create_callback = gridbuilder_create_callback
        self.gridbuilder_ready_callback = gridbuilder_ready_callback
        self.columns_ready_callback = columns_ready_callback
        self._reports = {}
        self._col_captions = {}

    def add_report(self, report_type, row_groups, col_groups, metrics, col_key_to_caption={}):
        self._reports[report_type] = {
            "row_groups": row_groups,
            "col_groups": col_groups,
            "metrics": metrics
        }
        self._col_captions = col_key_to_caption

    def report_keys(self):
        return list(self._reports.keys())

    def get_caption(self, key):
        return self._col_captions.get(key, key)

    def build(self, df, js_decimals, height):
        report_keys = self.report_keys()
        default_report = report_keys[0] if len(report_keys) > 0 else None
        report_type = st.pills("Report Type", options=report_keys, key=f"{self.control_key}_pills", label_visibility="collapsed", default=default_report)
        if report_type is None:
            report_type = default_report

        if report_type:
            self.df = df
            return self._make_pivot(df, report_type, js_decimals, height)

    def _make_grid_builder(self, report_type, df):
        if self.gridbuilder_create_callback is not None:
            result = self.gridbuilder_create_callback(self, report_type, df)
            return result
        return create_grid_builder(df)

    def on_columns_ready(self, key, report_type, col_to_def_dict: Dict[str, Dict[str, Any]]):
        if self.columns_ready_callback is not None:
            self.columns_ready_callback(self, report_type, col_to_def_dict)

    def _make_pivot(self, df, report_type, js_decimals, height):
        from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

        groups_rows = self._reports[report_type]["row_groups"]
        groups_cols = self._reports[report_type]["col_groups"]
        metrics_cols = self._reports[report_type]["metrics"]

        width_std = 60
        value_formatter = get_js_numberformat(js_decimals)
        col_to_def_dict = {}

        for c in df.columns:
            col_def = {
                "initialWidth": width_std,
                "wrapHeaderText": False,
                "suppressHeaderMenuButton": True,
                "pivot": False,
                "rowGroup": False,
                "aggFunc": ""
            }
            col_def["headerName"] = self.get_caption(c)

            if c in metrics_cols:
                col_def["enableValue"] = True
                col_def["valueFormatter"] = value_formatter
                col_def["filter"] = "agNumberColumnFilter"
                col_def["filterParams"] = {"defaultOption": "greaterThan"}
            else:
                col_def["enableRowGroup"] = True
                col_def["enablePivot"] = True
                col_def["filter"] = "agTextColumnFilter"
                col_def["filterParams"] = {"defaultOption": "contains"}

            col_to_def_dict[c] = col_def

            if c in groups_rows:
                col_def["rowGroup"] = True
                col_def["rowGroupIndex"] = groups_rows.index(c)

            if c in groups_cols:
                col_def["pivot"] = True
                col_def["pivotIndex"] = groups_cols.index(c)

            if c in metrics_cols:
                col_def["aggFunc"] = "sum"

        gb = self._make_grid_builder(report_type, df)
        if self.gridbuilder_ready_callback is not None:
            self.gridbuilder_ready_callback(self, report_type, gb, df)

        # Sort groups_rows, groups_cols, metric_cols first
        sorted_columns = sorted(col_to_def_dict.keys(), key=lambda c: (
            c not in groups_rows,  # Group rows first
            c not in groups_cols,  # Then group columns
            c not in metrics_cols,  # Then metrics columns
            c  # Finally, sort by column name
        ))

        self.on_columns_ready(self.control_key, report_type, col_to_def_dict)

        for c in sorted_columns:
            #pass
            gb.configure_column(c, 'aaa', **col_to_def_dict[c])

        grid_options = gb.build()
        AgGrid(df, gridOptions=grid_options, key=f"{self.control_key}_grid", allow_unsafe_jscode=True, enable_enterprise_modules=True, height=height)
        #AgGrid(df)



###
### Example usage of the PivotBuilder
###
def _make_test_pivot_data():
    import numpy as np
    num_rows = 20000
    categories = [f'Category {i:02d}' for i in range(1, 100)]
    subcategories = [f'Subcategory {i:02d}' for i in range(1, 10)]

    np.random.seed(42)
    data = {
        'Category': np.random.choice(categories, num_rows),
        'Subcategory': np.random.choice(subcategories, num_rows),
        'Value1': np.random.randint(1, 100, num_rows),
        'Value2': np.random.uniform(0, 100, num_rows).round(2),
        'Count': np.ones(num_rows, dtype=int),
    }

    df = pd.DataFrame(data)
    df = df.sort_values(['Category', 'Subcategory']).reset_index(drop=True)
    return df

def _create_grid_builder(pb: PivotBuilder, report_type: str, df: pd.DataFrame):
    return create_grid_builder(df, minWidth=200)

def _gridbuilder_ready_callback(pb: PivotBuilder, report_type: str, gb: Any, df: pd.DataFrame):
    logger.info(f"GridBuilder ready for report type: {report_type}")

def _columns_ready_callback(pb: PivotBuilder, report_type: str, col_to_def_dict: Dict[str, Dict[str, Any]]):
    logger.info(f"Columns ready for report type: {report_type}")
    if report_type == 'Test Report 2':
        for col in col_to_def_dict:
            col_to_def_dict[col]['initialWidth'] = 150

def _make_test_pivot():
    df = _make_test_pivot_data()
    pb = PivotBuilder(control_key="test_pivot", df=df,
                      gridbuilder_create_callback=_create_grid_builder, gridbuilder_ready_callback=_gridbuilder_ready_callback, columns_ready_callback=_columns_ready_callback)

    captions = {"Value1": "Value 1", "Value2": "Value 2"}
    metric_cols = ["Value1", "Value2", "Count"]
    pb.add_report(report_type="Test Report 1", row_groups=["Category", "Subcategory"], col_groups=[], metrics=metric_cols, col_key_to_caption=captions)
    pb.add_report(report_type="Test Report 2", row_groups=["Category"], col_groups=["Subcategory"], metrics=metric_cols, col_key_to_caption=captions)

    pb.build(df, js_decimals=2, height=650)

if __name__ == "__main__":
    import utils.streamlit_utils as stu
    stu.launch_streamlit("Pivot Builder Example", _make_test_pivot)