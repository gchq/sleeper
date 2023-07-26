/*
 * Copyright 2022-2023 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sleeper.build.dependencydraw;

import com.google.common.base.Function;
import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.DefaultModalGraphMouse;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.Renderer;

import sleeper.build.maven.DependencyReference;
import sleeper.build.maven.MavenModuleAndPath;
import sleeper.build.maven.MavenModuleStructure;

import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.SwingUtilities;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Paint;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DrawDependencyGraph {
    public boolean showTransitiveDependencies = false;

    public GraphData createGraph(List<MavenModuleAndPath> dependencies) {
        List<String> nodeIds = new ArrayList<>();
        List<List<String>> edges = new ArrayList<>();
        for (MavenModuleAndPath maven : dependencies) {
            nodeIds.add(maven.artifactReference().toString());
            List<DependencyReference> nodeDependencies = maven.dependencies().
                    filter(DependencyReference::isSleeper)
                    .filter(DependencyReference::isExported)
                    .collect(Collectors.toList());
            for (DependencyReference dependency : nodeDependencies) {
                List<String> listToAdd = new ArrayList<>();
                listToAdd.add(maven.artifactReference().toString());
                listToAdd.add(dependency.artifactReference().toString());
                edges.add(listToAdd);
            }
        }
        return new GraphData(nodeIds, edges);
    }

    public NodeData getEdges(VisualizationViewer vv, Graph g) {
        List<List<String>> allInEdges = new ArrayList<>();
        List<List<String>> allOutEdges = new ArrayList<>();
        Set<Integer> pickedNodes = vv.getPickedVertexState().getPicked();
        List<Integer> selectedNodesList = pickedNodes.stream().collect(Collectors.toList());

        for (int i = 0; i < selectedNodesList.size(); i++) {
            allInEdges.add(new ArrayList<>(g.getInEdges(String.valueOf(selectedNodesList.get(i)))));
            allOutEdges.add(new ArrayList<>(g.getOutEdges(String.valueOf(selectedNodesList.get(i)))));
        }
        return new NodeData(allInEdges, allOutEdges);
    }

    public NodeData getEdgesFromName(List<String> selectedNodesList, Graph g) {
        List<List<String>> allInEdges = new ArrayList<>();
        List<List<String>> allOutEdges = new ArrayList<>();

        for (int i = 0; i < selectedNodesList.size(); i++) {
            allInEdges.add(new ArrayList<>(g.getInEdges(String.valueOf(selectedNodesList.get(i)))));
            allOutEdges.add(new ArrayList<>(g.getOutEdges(String.valueOf(selectedNodesList.get(i)))));
        }
        return new NodeData(allInEdges, allOutEdges);
    }

    public void drawGraph(GraphData graphData) {
        List<String> nodeIDs = graphData.getNodeIds();
        List<List<String>> edges = graphData.getEdges();
        Graph<String, String> g = new DirectedSparseGraph<>();
        for (String node : nodeIDs) {
            g.addVertex(node);
        }
        for (List<String> edge : edges) {
            g.addEdge(edge.get(0) + "---" + edge.get(1), edge.get(0), edge.get(1));
        }
        Layout<Integer, String> layout = new CircleLayout(g);
        layout.setSize(new Dimension(900, 900));
        VisualizationViewer<Integer, String> vv = new VisualizationViewer<Integer, String>(layout);
        DefaultModalGraphMouse gm = new DefaultModalGraphMouse();
        Function<String, Paint> edgePaint = s -> {
            return calculateEdgeColor(s, vv, g);
        };

        Function<String, Paint> arrowPaint = s -> {
            return calculateArrowColor(s, edgePaint);
        };

        JFrame frame = new JFrame("Dependency Graph View");
        JLabel text = new JLabel("P - Selection Mode | T - Traverse mode |\n");
        JLabel text2 = new JLabel("Red - Going to | Blue - Going from");
        JCheckBox transitiveCheckBox = new JCheckBox("Show transitive dependencies");
        transitiveCheckBox.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if (e.getStateChange() == 1) {
                    showTransitiveDependencies = true;
                } else {
                    showTransitiveDependencies = false;
                }
                SwingUtilities.updateComponentTreeUI(frame);
            }
        });

        vv.setPreferredSize(new Dimension(350, 350));
        vv.getRenderContext().setEdgeDrawPaintTransformer(edgePaint);
        vv.getRenderContext().setArrowDrawPaintTransformer(arrowPaint);
        vv.getRenderContext().setArrowFillPaintTransformer(arrowPaint);
        vv.getRenderContext().setVertexLabelTransformer(new ToStringLabeller());
        vv.getRenderer().getVertexLabelRenderer().setPosition(Renderer.VertexLabel.Position.CNTR);
        vv.setGraphMouse(gm);
        vv.addKeyListener(gm.getModeKeyListener());
        vv.setVertexToolTipTransformer(new ToStringLabeller());
        vv.setEdgeToolTipTransformer(new ToStringLabeller());
        gm.setMode(ModalGraphMouse.Mode.TRANSFORMING);
        vv.add(text, BorderLayout.CENTER);
        vv.add(text2, BorderLayout.CENTER);
        vv.add(transitiveCheckBox);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(vv);
        frame.pack();
        frame.setVisible(true);
    }

    public void produceGraphFromMaven(MavenModuleStructure maven) {
        drawGraph(
                createGraph(
                        maven.allModules().collect(Collectors.toList())
                )
        );
    }

    public Color calculateEdgeColor(String s, VisualizationViewer vv, Graph g) {
        List<List<String>> edgesList = getEdges(vv, g).getInEdges();
        List<List<String>> edgedOutList = getEdges(vv, g).getOutEdges();
        List<String> nextNodes = new ArrayList<>();
        for (Collection<String> edgeIn : edgedOutList) {
            for (String edge : edgeIn) {
                nextNodes.add(String.valueOf(edge).split("---")[1]);
            }
        }
        List<List<String>> edgeNames = getEdgesFromName(nextNodes, g).getOutEdges();
        for (Collection<String> edgeIn : edgesList) {
            if (edgeIn.contains(s)) {
                return Color.RED;
            } else {
                for (Collection<String> edgeOut : edgedOutList) {
                    if (edgeOut.contains(s)) {
                        return Color.BLUE;
                    } else {
                        if (showTransitiveDependencies) {
                            for (int k = 0; k < edgeNames.size(); k++) {
                                List<String> nextOutEdge = edgeNames.get(k);
                                if (nextOutEdge.contains(s)) {
                                    return Color.BLACK;
                                }
                            }
                        }
                        return Color.lightGray;
                    }
                }
            }
        }
        return Color.BLACK;
    }

    public Paint calculateArrowColor(String s, Function<String, Paint> edgePaint) {
        Paint edgePaintColor = edgePaint.apply(s);
        if (edgePaintColor.equals(Color.lightGray)) {
            return new Color(255, 255, 255, 0);
        }
        return edgePaintColor;
    }
}
