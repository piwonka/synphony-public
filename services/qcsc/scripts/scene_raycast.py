import bpy  # might be flagged as an error in ide, dependency available at runtime
from mathutils import Vector
import json

# find the camera object
camera = bpy.data.objects.get('Camera')

# retrieve camera location to ensure raycast starts at the correct place
ray_origin = camera.location

# compute the direction of the raycast using the cameras world coordinates and multiplying its quarternion with a normalized vector in z direction (raycast down because camera is above terrain)
ray_direction = (camera.matrix_world.to_quaternion()
                 @ Vector((0, 0, -1))).normalized()

# get the scenes dependency graph ( needed for raycast hit detection)
depsgraph = bpy.context.evaluated_depsgraph_get()

# compute the raycast, ignore everything in the results after location
hit, location, normal, index, obj, mtx = bpy.context.scene.ray_cast(
    depsgraph, ray_origin, ray_direction)


# return quality gate result
print("RESULT,"+seed+",SCENE_IN_CAMERA"+","+str(hit) +
      "," + str(location.to_tuple()), flush=True)
