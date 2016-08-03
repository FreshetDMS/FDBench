<!DOCTYPE html>
<html lang="en">
<head>
    <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css"
          integrity="sha384-BVYiiSIFeK1dGmJRAkycuHAHRg32OmUcww7on3RYdg4Va+PmSTsz/K68vbdEjh4u" crossorigin="anonymous">
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.1.0/jquery.min.js"></script>
    <script src="https://code.highcharts.com/highcharts.js"></script>
    <style type="text/css">
        body {
            padding-top: 50px;
        }

        .t-plot {
            height: 450px;
            width: 100%;
        }
    </style>
</head>
<body>
<nav class="navbar navbar-default navbar-fixed-top">
    <div class="container-fluid">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
                <span class="sr-only">Toggle navigation</span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand" href="#">${benchmark} - Plots</a>
        </div>
    </div><!--/.container-fluid -->
</nav>
<div class="container">

    <h1>Latency</h1>
    <div class="row">
        <div class="col-md-12">
            <div class="t-plot" id="success-latency-plot"></div>
        </div>
        <script type="text/javascript">
            $(function() {
                $('#success-latency-plot').highcharts({
                    title : {
                        text: 'Produce Latency'
                    },
                    legend: {
                        enabled: false
                    },
                    xAxis: {
                        categories: [<#list successLabels as l>"${l}"<#if l?index != successLabels?size - 1>,</#if></#list>]
                    },
                    yAxis: {
                        title: {
                            text: 'Latency (ns)'
                        }
                    },
                    series: [{
                        data: [<#list successLatencies as v>${v?string.computer}<#if v?index != successLatencies?size - 1>,</#if></#list>]
                    }],
                    plotOptions: {
                        line: {
                            lineWidth: 2,
                            marker: {
                                enabled: false
                            }
                        }
                    },
                })
            });
        </script>
    </div>


    <h1>Memory Usage</h1>
    <#list containermemusage as c>
    <div class="row">
        <div class="col-md-12">
            <canvas class="t-plot" id="mem-plot-${c.container}"></canvas>
        </div>
        <script type="text/javascript">
            $(function() {
                $('#mem-plot-${c.container}').highcharts({
                    title : {
                        text: 'Used Memory - ${c.container}'
                    },
                    legend: {
                        enabled: false
                    },
                    xAxis: {
                        categories: [<#list c.timestamps as l>"${l}"<#if l?index != c.timestamps?size - 1>,</#if></#list>]
                    },
                    yAxis: {
                        title: {
                            text: 'Latency (ns)'
                        }
                    },
                    series: [{
                        data: [<#list c.usedmem as m>${m?string.computer}<#if m?index != c.usedmem?size - 1>,</#if></#list>]
                    }],
                    plotOptions: {
                        line: {
                            lineWidth: 2,
                            marker: {
                                enabled: false
                            }
                        }
                    },
                })
            });
        </script>
    </div>
    </#list>
</div>
</body>
</html>