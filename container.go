package ok

import (
	"io"

	"github.com/fsouza/go-dockerclient"
)

type ContainerOptions struct {
}

func Container(endpoint string, v Version) (*container, error) {
	client, err := docker.NewClient(endpoint)
	if err != nil {
		return nil, err
	}
	return &container{
		client: client,
	}, nil
}

type container struct {
	ID     string
	client *docker.Client
}

func (c *container) UploadToContainer(path string, input io.Reader) {
	c.client.UploadToContainer(c.ID, docker.UploadToContainerOptions{
		Path:        path,
		InputStream: input,
	})
}

func (c *container) Destroy() error {
	c.client.StopContainer(c.ID, 0)
	return c.client.RemoveContainer(docker.RemoveContainerOptions{
		ID:            c.ID,
		Force:         true,
		RemoveVolumes: true,
	})
}
