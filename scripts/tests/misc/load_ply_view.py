import bpy
# Import PLY
bpy.ops.import_mesh.ply(filepath="/home/chris/Desktop/Master/debug/scan.ply")

obj = bpy.context.selected_objects[0]
bpy.context.view_layer.objects.active = obj
