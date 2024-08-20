import sys
import siibra
from siibra.atlases.sparsemap import SparseIndex
from tqdm import tqdm

def create_sparse_index(parc_id: str, space_id: str, filename: str, maptype: str="statistical", extra_spec: str=""):
    mp = siibra.get_map(parc_id, space_id, maptype, extra_spec)

    spi = SparseIndex(filename, mode="w")
    
    progress = tqdm(total=len(mp.regions), leave=True)
    for regionname in mp.regions:
        volumes = mp.find_volumes(regionname)
        assert len(volumes) == 1
        volume = volumes[0]
        spi.add_img(volume.fetch(), regionname)
        progress.update(1)
    progress.close()
    spi.save()

if __name__ == "__main__":
    create_sparse_index(*sys.argv[1:])
