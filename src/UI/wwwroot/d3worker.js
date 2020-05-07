importScripts('d3/d3.js');

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms))

self.onmessage = async function(e) {
    if (e.data.type === 'initial') initial(e)
    if (e.data.type === 'update') updateGraph()
    if (e.data.type === 'drag')  drag(e)
}

let simulation, nodes, links
function drag(e){
    dragged = e.data.dragged;
    nodes[dragged.index].fx = dragged.fx;
    nodes[dragged.index].fy = dragged.fy;
}
function initial(e) {
    nodes = e.data.nodes;
    links = e.data.links;
    width = e.data.width;
    height = e.data.height;

    // Add force simulation
    simulation = d3.forceSimulation(nodes)
        .force('charge', d3.forceManyBody().strength(-500)) // -800
        .force('link', d3.forceLink(links).distance(50).id(d => d.source))
        .force('collide', d3.forceCollide().radius(10))
        .force('x', d3.forceX(width/2))
        .force('y', d3.forceY(height/2))
        .stop();

    postMessage({type: "initial", nodes, links });
}

function updateGraph() {
    simulation.tick();
    postMessage({type: "update", nodes, links });
}