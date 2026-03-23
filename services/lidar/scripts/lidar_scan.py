# Imports may be tagges as errors, bpy and range-scanner are present at runtime
import bpy
import range_scanner

print("USER SCRIPT OUTPUT", INPUT_FILE,
      OUTPUT_PATH, OUTPUT_FILE)  # Debugging output
# load the provided blend file for the job
bpy.ops.wm.open_mainfile(filepath=INPUT_FILE)
# Velodyne (LIDAR)
range_scanner.ui.user_interface.scan_rotating(
    bpy.context,
    scannerObject=bpy.context.scene.objects["Camera"],
    xStepDegree=0.2,  # 0.1 for higher workload
    fovX=30.0,
    yStepDegree=0.2,  # 0.1 for higher workload
    fovY=30.0, rotationsPerSecond=20,
    reflectivityLower=0.0, distanceLower=0.0, reflectivityUpper=0.0, distanceUpper=99999.9, maxReflectionDepth=10,
    enableAnimation=False, frameStart=1, frameEnd=1, frameStep=1, frameRate=1,
    addNoise=False, noiseType='gaussian', mu=0.0, sigma=0.01, noiseAbsoluteOffset=0.0, noiseRelativeOffset=0.0,
    simulateRain=False, rainfallRate=0.0,
    addMesh=True,
    exportLAS=False, exportHDF=False, exportCSV=False, exportPLY=True, exportSingleFrames=False,
    dataFilePath=OUTPUT_PATH, dataFileName=OUTPUT_FILE,
    debugLines=False, debugOutput=False, outputProgress=True, measureTime=False, singleRay=False, destinationObject=None, targetObject=None
)
