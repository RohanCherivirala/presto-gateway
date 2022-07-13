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
            $('#routingGroups').dataTable( {
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
            document.getElementById("routing_groups_tab").style.backgroundColor = "grey";
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
    <h3>Routing Groups:<h3>
    <table id="routingGroups" class="display">
        <thead>
            <tr>
                <th>Group Name</th>
                <th>Active Clusters</th>
                <th>Number Of Clusters</th>
                <th>Change Status</th>
                <th>Status</th>
            </tr>
        </thead>

        <tbody>
            <#list routingGroupConfigurations as routingGroup>
                <tr>
                    <td> ${routingGroup.name}</td>
                    <td> ${routingGroup.activeClusters}</td>
                    <td> ${routingGroup.numberOfClusters}</td>

                    <#if routingGroup.active == true>
                        <td><button onclick = 'pauseRoutingGroup("${routingGroup.name}")'>Pause Routing Group</button></td>
                        <td class = "activeCell">active</td>
                    <#else>
                        <td><button onclick = 'resumeRoutingGroup("${routingGroup.name}")'>Resume Routing Group</button></td>
                        <td class = "disabledCell">paused</td>
                    </#if>
                </tr>
            </#list>
        </tbody>
    </table>
</div>

<#include "footer.ftl">