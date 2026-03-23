import os
import bpy  # might be flagged as an error by IDE, dependency available a runtime
import json

# retrieve scene dependency graph
depsgraph = bpy.context.evaluated_depsgraph_get()

# initialize variables for vertice and triangle count
verts = 0
tris = 0

# loop over objects in the scene
for obj in bpy.data.objects:
    if obj.type != "MESH":  # skip non-mesh objects
        continue
    if obj.hide_render:  # skip hidden objects
        continue
    obj_eval = obj.evaluated_get(depsgraph)  # evaluate the mesh
    me = obj_eval.to_mesh()  # create a temporary mesh datablock for it
    try:
        me.calc_loop_triangles()  # generates triangulated view without modifying mesh
        verts += len(me.vertices)  # add vertices of mesh to sum
        tris += len(me.loop_triangles)  # add triangles of mesh to sum
    finally:
        obj_eval.to_mesh_clear()  # force free the temporary mesh data block

    # retrieve quality gate parameters using env variables
    verts_lower_bound = os.environ['QC_VERTS_LOWER_BOUND']
    tris_lower_bound = os.environ['QC_TRIS_LOWER_BOUND']

    # compute quality gate decision using the computed verts and tris and the env parameters
    decision = verts > int(verts_lower_bound) and tris > int(tris_lower_bound)

    # wrap computed sums in json object
    data = json.dumps({"verts": verts, "tris": tris})

    # return quality gate result
    print("RESULT,"+seed+",TRIANGLE_VERTEX_COUNT" +
          ","+str(decision)+","+str(data), flush=True)
