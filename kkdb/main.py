# Main Entry Point for the KKDB Application
import hydra

@hydra.main(config_path="configs/config.yaml")
def main(cfg):
    print(cfg)