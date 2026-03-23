import bpy  # may be flagged as error by ide, dependency available at runtime
import random
import math
bpy.ops.wm.read_factory_settings(use_empty=True)  # load an empty scene

# use ant_landscape to generate a terrain
bpy.ops.preferences.addon_enable(module="ant_landscape")

area = next(
    area for area in bpy.context.window.screen.areas if area.type == 'VIEW_3D')
region = next(region for region in area.regions if region.type == 'WINDOW')

override = {'area': area, 'region': region,
            'window': bpy.context.window, 'screen': bpy.context.screen}


mat_landscape = bpy.data.materials.new(name="LandscapeMat")


mat_landscape.use_nodes = True
mat_landscape.node_tree.nodes["Principled BSDF"].inputs[0].default_value = (
    0.2, 0.6, 0.2, 1)

bpy.ops.mesh.landscape_add(
    refresh=True,
    subdivision_x=128,
    subdivision_y=128,
    mesh_size_x=20,
    mesh_size_y=20,
    noise_size=2.0,
    noise_depth=6,
    noise_type='fractal',
    # random usage is controlled through previous seeding
    random_seed=random.randint(0, 9999)
)

# use terrain as active object
landscape_obj = bpy.context.active_object


# Subsurface (complex scene)
# 2=x64 3=x128 vertices
# sub = landscape_obj.get("Subdivision") or landscape_obj.modifiers.new("Subdivision", "SUBSURF")
# sub.subdivision_type = 'SIMPLE'  # keep terrain shape, no smoothing
# sub.levels = 2  # 4=x256
# sub.render_levels = 2  # 4 = x256

bpy.context.view_layer.objects.active = landscape_obj
for m in list(landscape_obj.modifiers):
    bpy.ops.object.modifier_apply(modifier=m.name)

# remove any materials ( there should be none but better safe than sorry)
landscape_obj.data.materials.clear()
# give the landscape proper materials
landscape_obj.data.materials.append(mat_landscape)
landscape_obj.location = (0.0, 0.0, 0.0)


# Add a Camera
cam = bpy.data.objects.get("Camera")
if cam is None:
    bpy.ops.object.camera_add()
    cam = bpy.context.active_object
    cam.name = "Camera"

# Put camera above the terrain and point it down
cam.location = (0.0, 0.0, 20.0)
cam.rotation_euler = (0.0, 0.0, 0.0)  # point down along -Z
# Make it the active scene came
bpy.context.scene.camera = cam


# Save file
output = OUTPUT_FILE
bpy.ops.wm.save_mainfile(filepath=output)
print(f"Saved:{output}", flush=True)
