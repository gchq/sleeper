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

package sleeper.build.maven.dependencydraw;

import org.jgrapht.Graph;
import org.jgrapht.graph.builder.GraphTypeBuilder;
import org.jungrapht.visualization.VisualizationViewer;
import org.jungrapht.visualization.decorators.EllipseShapeFunction;
import org.jungrapht.visualization.decorators.IconShapeFunction;
import org.jungrapht.visualization.layout.algorithms.SugiyamaLayoutAlgorithm;
import org.jungrapht.visualization.renderers.JLabelEdgeLabelRenderer;
import org.jungrapht.visualization.renderers.JLabelVertexLabelRenderer;
import org.jungrapht.visualization.renderers.Renderer;
import org.jungrapht.visualization.util.AWT;
import org.jungrapht.visualization.util.IconCache;

import javax.swing.*;
import javax.swing.border.Border;
import javax.swing.border.CompoundBorder;

import java.awt.*;
import java.awt.event.ItemEvent;
import java.awt.geom.AffineTransform;

public class DrawDependencyGraph {
    public boolean showTransitiveDependencies = false;

    public void drawGraph(GraphModel model) {
        Graph<GraphNode, GraphEdge> g =
                GraphTypeBuilder.<GraphNode, GraphEdge>directed().buildGraph();
        model.getNodes().forEach(g::addVertex);
        model.getEdges().forEach(edge -> g.addEdge(edge.getFrom(model), edge.getTo(model), edge));

        Dimension size = new Dimension(600, 600);
        VisualizationViewer<GraphNode, GraphEdge> vv =
                VisualizationViewer.builder(g)
                        .viewSize(size)
                        .layoutSize(size).build();

        IconCache<GraphNode> iconCache =
                IconCache.<GraphNode>builder(
                                n -> "<html>"+n.toString().replaceAll("/", "<br>")
                        )
                        .vertexShapeFunction(vv.getRenderContext().getVertexShapeFunction())
                        .colorFunction(
                                n -> {
                                    if (g.degreeOf(n) > 9) return Color.red;
                                    if (g.degreeOf(n) < 7) return Color.green;
                                    return Color.lightGray;
                                })
                        .stylist(
                                (label, vertex, colorFunction) -> {
                                    label.setFont(new Font("Serif", Font.BOLD, 20));
                                    label.setForeground(Color.black);
                                    label.setBackground(Color.white);
                                    Border lineBorder =
                                            BorderFactory.createEtchedBorder(); //Border(BevelBorder.RAISED);
                                    Border marginBorder = BorderFactory.createEmptyBorder(4, 4, 4, 4);
                                    label.setBorder(new CompoundBorder(lineBorder, marginBorder));
                                })
                        .preDecorator(
                                (graphics, vertex, labelBounds, vertexShapeFunction, colorFunction) -> {
                                    // save off the old color
                                    Color oldColor = graphics.getColor();
                                    // fill the image background with white
                                    graphics.setPaint(Color.white);
                                    graphics.fill(labelBounds);

                                    Shape shape = vertexShapeFunction.apply(vertex);
                                    Rectangle shapeBounds = shape.getBounds();

                                    AffineTransform scale =
                                            AffineTransform.getScaleInstance(
                                                    1.3 * labelBounds.width / shapeBounds.getWidth(),
                                                    1.3 * labelBounds.height / shapeBounds.getHeight());
                                    AffineTransform translate =
                                            AffineTransform.getTranslateInstance(
                                                    labelBounds.width / 2., labelBounds.height / 2.);
                                    translate.concatenate(scale);
                                    shape = translate.createTransformedShape(shape);
                                    graphics.setColor(Color.pink);
                                    graphics.fill(shape);
                                    graphics.setColor(oldColor);
                                })
                        .build();

        vv.getRenderContext().setVertexLabelRenderer(new JLabelVertexLabelRenderer(Color.cyan));
        vv.getRenderContext().setEdgeLabelRenderer(new JLabelEdgeLabelRenderer(Color.cyan));

        final IconShapeFunction<GraphNode> vertexImageShapeFunction =
                new IconShapeFunction<>(new EllipseShapeFunction<>());
        vertexImageShapeFunction.setIconFunction(iconCache);
        vv.getRenderContext().setVertexShapeFunction(vertexImageShapeFunction);
        vv.getRenderContext().setVertexIconFunction(iconCache);


        SugiyamaLayoutAlgorithm<GraphNode, GraphEdge> layout =
                new SugiyamaLayoutAlgorithm<>();
        layout.setVertexBoundsFunction(
                v -> (org.jungrapht.visualization.layout.model.Rectangle)
                        vv.getRenderContext().getVertexShapeFunction().andThen(s -> AWT.convert(s.getBounds2D())));
        vv.getVisualizationModel().setLayoutAlgorithm(layout);

        PickedNodeState picked = new PickedNodeState(model);
        vv.getRenderContext().getSelectedVertexState().addItemListener(event ->
                picked.updatePicked(vv.getRenderContext().getSelectedVertexState().getSelected()));
        vv.getRenderContext().setEdgeDrawPaintFunction(edge -> picked.calculateEdgeColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowDrawPaintFunction(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setArrowFillPaintFunction(edge -> picked.calculateArrowColor(edge, showTransitiveDependencies));
        vv.getRenderContext().setVertexDrawPaintFunction(node -> new Color(255, 255, 255, 0));
        vv.getRenderContext().setVertexLabelPosition(Renderer.VertexLabel.Position.CNTR);
        vv.setVertexToolTipFunction(Object::toString);
        vv.setEdgeToolTipFunction(Object::toString);

        JFrame frame = new JFrame("Dependency Graph View");
        JLabel text = new JLabel("P - Selection Mode | T - Traverse mode |\n");
        JLabel text2 = new JLabel("Red - Going to | Blue - Going from");
        JCheckBox transitiveCheckBox = new JCheckBox("Show transitive dependencies");
        transitiveCheckBox.addItemListener(e -> {
            showTransitiveDependencies = e.getStateChange() == ItemEvent.SELECTED;
            SwingUtilities.updateComponentTreeUI(frame);
        });
        vv.add(text, BorderLayout.CENTER);
        vv.add(text2, BorderLayout.CENTER);
        vv.add(transitiveCheckBox);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(vv.getComponent());
        frame.pack();
        frame.setVisible(true);
        frame.requestFocus();
    }
}
