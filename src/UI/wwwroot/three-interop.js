window.threeInterop = {
    graph : {},
    example: function example(divId, data) {
        
        var ctr = 0;
        const nodes = data.map(d => {return {id:d.source, edges:d.edges.length}} );
        const links = data.map(d =>
            d.edges.map(typ => {
                ctr = ctr + 1;
                var fjdsk = typ.target.map(t => {
                    return {
                        "source": d.source,
                        "target": t,
                        "type" : typ.type,
                        "id" : `${ctr}`
                    }});
                return fjdsk;
            }).flat()).flat()
            .filter(l => {
                return nodes.find(n => n.id === l.target);
            });

        const elem = document.getElementById(divId);

        this.graph = ForceGraph3D()(elem)
            .enableNodeDrag(false)
            .width(elem.clientWidth)
            .height(elem.clientHeight)
            .backgroundColor("#111111")
            .graphData({nodes:nodes, links:links})
            .linkLabel(link => link.type)
            .linkAutoColorBy(link => link.type)
            .linkCurvature(0.33)
            .linkWidth(3)
            .linkDirectionalArrowLength(6)
            .nodeAutoColorBy('edges')
            .nodeLabel(node => `${node.id}`)
            .onNodeHover(node => elem.style.cursor = node ? 'pointer' : null);
            //.onNodeClick(node => window.open(`https://bl.ocks.org/${node.user}/${node.id}`, '_blank'));
    }
}