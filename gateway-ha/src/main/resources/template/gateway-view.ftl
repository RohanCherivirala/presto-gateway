<#-- @ftlvariable name="" type="com.lyft.data.gateway.resource.GatewayViewResource$GatewayView" -->
<#setting datetime_format = "MM/dd/yyyy hh:mm:ss a '('zzz')'">
    <html>
<head>
    <meta charset="UTF-8"/>
    <style>
        .pull-left {
            float: left !important
        }

        .pull-right {
            float: right !important
        }

        .dataTables_filter input {
            width: 500px
        }
    </style>

    <link rel="stylesheet" type="text/css" href="assets/css/common.css"/>
    <link rel="stylesheet" type="text/css" href="assets/css/jquery.dataTables.min.css"/>

    <script src="assets/js/jquery-3.3.1.js"></script>
    <script src="assets/js/jquery.dataTables.min.js"></script>
    <script src="assets/js/hbar-chart.js"></script>
    <script src="assets/js/changeStatus.js"></script>

    <link rel="stylesheet" type="text/css" href="assets/css/tableProperties.css"/>

    <script type="application/javascript">
        $(document).ready(function () {
            $('#clusters').dataTable( {
                "ordering": false,
                "dom": '<"pull-left"f><"pull-right"l>tip',
                "width": '100%',
                "columnDefs": [ {
                    "targets":  3,
                    "searchable": false
                },
                {
                    "targets": '_all',
                    "createdCell": function (td, cellData, rowData, row, col) {
                        $(td).css('padding', '8px 18px')
                    }
                }]
            } );

            $("ul.chart").hBarChart();
            document.getElementById("active_backends_tab").style.backgroundColor = "grey";
        });
    </script>
</head>
<body>
<#include "header.ftl">
<div>
    Started at :
    <script>document.write(new Date(${gatewayStartTime?long?c}).toLocaleString());</script>
</div>

<div>
    <h3>All backends:</h3>
    <table id="clusters" class="display">
        <thead>
            <tr>
                <th>ClusterName</th>
                <th>URL</th>
                <th>RoutingGroup</th>
                <th>Change Status</th>
                <th>Status</th>
            </tr>
        </thead>

        <tbody>
            <#list backendConfigurations as bc>
                <tr>
                    <td>  ${bc.name}</td>
                    <td><a href="${bc.proxyTo}/ui/" target="_blank">${bc.proxyTo}</a></td>
                    <td> ${bc.routingGroup}</td>

                    <#if bc.active == true>
                        <td><button onclick = 'disableCluster("${bc.name}")'>Disable Cluster</button></td>
                        <td class = "activeCell">active</td>
                    <#else>
                        <td><button onclick = 'activateCluster("${bc.name}")'>Activate Cluster</button></td>
                        <td class = "disabledCell">disabled</td>
                    </#if>
                </tr>
            </#list>
        </tbody>
    </table>
</div>

<#include "footer.ftl">