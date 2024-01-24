# Computer Architectures, Project 4 (2023 fall)

Implement FAST page mapping in SimpleSSD simulator. Course project 4 for Computer Architectures course in Peking University (2023 fall).

The main implementation of FAST page mapping is in ```simplessd/ftl/fast.cc``` and ```simplessd/ftl/fast.hh```. I also modified or added some functions in ```simplessd/ftl/common/block.cc``` and ```simplessd/ftl/common/block.hh```. To start simulation using FAST page mapping, first build SimpleSSD simulator:

```bash
git clone https://github.com/YK-YoungK/CA_proj4_SSD-Fast.git
cd CA_proj4_SSD-Fast
cmake -DDEBUG_BUILD=off .
make -j 8
```

Then, start simulation in the following format: ```simplessd-standalone <Simulation configuration file> <SimpleSSD configuration file> <Output directory>```. One example is:
```bash
./simplessd-standalone ./config/simulation_sample.cfg ./simplessd/config/ssd_sample.cfg .
```
You can change some other simulation settings (e.g., SSD size, I/O patterns) in ```simulation_sample.cfg``` and ```ssd_sample.cfg```. Note that after my modification, the simulator no longer supports superpage.
