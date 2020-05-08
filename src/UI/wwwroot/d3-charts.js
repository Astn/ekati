window.d3Charts = {
    chartsInteralData : {},
    updateCharts: function updateCharts(chartsData){
        // [{
        //  "id":string,
        //  "measures":[
        //    "time": "time string",
        //    "value": number
        //  ]
        // }]
        var	parseDate = d3.isoParse;
        chartsData.forEach(chartData => {
            // get the svg for this chart
            
            var svgId = "svg#"+chartData.id;
            var svg = d3.select(svgId);


            var width = svg.attr("width") | 600; 
            var height = svg.attr("height") | 300;
            
            var margin = ({top: 20, right: 0, bottom: 40, left: 53})
            var xRange = [margin.left, width - margin.right];
            var yRange = [height - margin.bottom, margin.top];

            chartData.dataset.forEach(function(d){
                d.date = parseDate(d.time);
            });

            // update ranges and domains
            if(this.chartsInteralData[chartData.id] === undefined){
                this.chartsInteralData[chartData.id] = {};
                this.chartsInteralData[chartData.id].y = d3.scaleLinear()
                    .domain([0, d3.max(chartData.dataset, d => d.value)])
                    .range(yRange);
                this.chartsInteralData[chartData.id].x = d3.scaleBand()
                    .rangeRound(xRange, 0.5)
                    .padding(0.1)
                    .domain(chartData.dataset.map(d => d.date));
            } else {
                this.chartsInteralData[chartData.id].y
                    .domain([0, d3.max(chartData.dataset, d => d.value)])
                    .range(yRange);
                this.chartsInteralData[chartData.id].x
                    .rangeRound(xRange, 0.5)
                    .domain(chartData.dataset.map( d => d.date));
            }
           
            // X axis
            const bottomAxis = svg.selectAll("g#"+chartData.id+"-axisBottom").data([{}]);
            bottomAxis
                .enter().append("g")
                    .attr ("id",chartData.id+"-axisBottom")
                    .attr("transform", `translate(0,${height - margin.bottom})`)
                    .call(d3.axisBottom(this.chartsInteralData[chartData.id].x))
                    .call(e => this.chartsInteralData[chartData.id].ba = e);

            this.chartsInteralData[chartData.id].ba
                .attr("transform", `translate(0,${height - margin.bottom})`)
                .transition().duration(500)
                .call(d3.axisBottom(this.chartsInteralData[chartData.id].x));
            
            // Y axis
            const leftAxis = svg.selectAll("g#"+chartData.id+"-axisLeft").data([{}]);
            leftAxis
                .enter().append("g")
                    .attr ("id",chartData.id+"-axisLeft")
                    .attr("transform", `translate(${margin.left},0)`)
                    .call(d3.axisLeft(this.chartsInteralData[chartData.id].y))
                    .call(e => this.chartsInteralData[chartData.id].la = e);

            this.chartsInteralData[chartData.id].la
                    .attr("transform", `translate(${margin.left},0)`)
                    .transition().duration(500)
                    .call(d3.axisLeft(this.chartsInteralData[chartData.id].y));
            
            // the dots
            svg.selectAll("g#"+chartData.id+"-data")
                    .data([{}])
                    .enter().append("g")
                        .attr("id", chartData.id+"-data")
                        .attr("fill", "steelblue");
            
            let points =  svg.select("g#"+chartData.id+"-data")
                    .selectAll("circle")
                    .data(chartData.dataset);
            
            points.enter()
                    .append("circle")
                    .attr("cx", d => this.chartsInteralData[chartData.id].x(d.date))
                    .attr("cy", d => this.chartsInteralData[chartData.id].y(d.value))
                    .attr("stroke-width", 1.5)
                    .attr("r", 2);
            
            points
                    .transition().duration(500)    
                    .attr("cx", d => this.chartsInteralData[chartData.id].x(d.date))
                    .attr("cy", d => this.chartsInteralData[chartData.id].y(d.value))
                    .attr("stroke-width", 1.5)
                    .attr("r", 2)
                    ;
            points.exit().remove();    

        });
    },
};