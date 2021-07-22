package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/docker/distribution/registry/storage"

	"github.com/docker/distribution/manifest/schema2"

	"github.com/docker/distribution"

	"github.com/opencontainers/go-digest"

	"github.com/docker/distribution/registry/proxy"

	"github.com/docker/distribution/registry/api/errcode"

	"github.com/docker/distribution/reference"

	newContext "github.com/docker/distribution/context"
	"github.com/gorilla/handlers"
)

type proxyHandler struct {
	*Context

	// One of tag or digest gets set, depending on what is present in context.
	Tag    string
	Digest digest.Digest
}

// proxyDispatcher uses the request context to build a proxyHandler
func proxyDispatcher(ctx *Context, r *http.Request) http.Handler {
	proxyHandler := &proxyHandler{
		Context: ctx,
	}
	reference := getReference(ctx)
	dgst, err := digest.Parse(reference)
	if err != nil {
		// We just have a tag
		proxyHandler.Tag = reference
	} else {
		proxyHandler.Digest = dgst
	}

	mhandler := handlers.MethodHandler{
		"DELETE": http.HandlerFunc(proxyHandler.DeleteProxyStore),
	}
	return mhandler
}

// DeleteProxyStore 删除逻辑
func (p *proxyHandler) DeleteProxyStore(w http.ResponseWriter, r *http.Request) {
	newContext.GetLogger(p).Info("DeleteProxyStore")
	p.App.registry = proxy.OlderRegistry
	manifests, err := getManifestsInfo(p)
	if err != nil {
		newContext.GetLogger(p).Errorf("获取manifests digest信息：%v", err)
		p.Errors = append(p.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
	// 打印digest信息
	name := getName(p.Context)
	newContext.GetLogger(p).Info("DeleteBlob")
	for _, val := range manifests.Layers {
		newContext.GetLogger(p).Info("Layer Digest信息:", val.Digest)
		err := delBlobFunc(fmt.Sprintf("%s@%s", name, val.Digest), p)
		if err != nil {
			continue
		}
	}
	newContext.GetLogger(p).Info("Manifests")
	err = delManifestFunc(fmt.Sprintf("%s@%s", name, manifests.Config.Digest), p)
	if err != nil {
		newContext.GetLogger(p).Errorf("删除manifests出错,错误信息：%v", err)
		p.Errors = append(p.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusOK)
}

func getManifestsInfo(p *proxyHandler) (*schema2.DeserializedManifest, error) {
	manifests, err := p.Repository.Manifests(p)
	if err != nil {
		return nil, err
	}
	var supports [numStorageTypes]bool
	supports[manifestSchema2] = true
	if p.Tag != "" {
		tags := p.Repository.Tags(p)
		desc, err := tags.Get(p, p.Tag)
		if err != nil {
			return nil, err
		}
		p.Digest = desc.Digest
	}
	var options []distribution.ManifestServiceOption
	if p.Tag != "" {
		options = append(options, distribution.WithTag(p.Tag))
	}
	manifest, err := manifests.Get(p, p.Digest, options...)
	if err != nil {
		return nil, err
	}
	schema2Manifest, _ := manifest.(*schema2.DeserializedManifest)
	return schema2Manifest, nil
}

// delManifestFunc 删除manifests
func delManifestFunc(key string, p *proxyHandler) error {
	ref, err := reference.Parse(key)
	if err != nil {
		return fmt.Errorf("Scheduler error returned from OnExpire(%s): %s", key, err)
	}
	var r1 reference.Canonical
	var ok bool
	if r1, ok = ref.(reference.Canonical); !ok {
		return fmt.Errorf("unexpected reference type : %T", ref)
	}
	repo, err := p.registry.Repository(context.TODO(), r1)
	if err != nil {
		return err
	}
	manifests, err := repo.Manifests(context.TODO())
	if err != nil {
		return err
	}
	err = manifests.Delete(context.TODO(), r1.Digest())
	if err != nil {
		return err
	}
	return nil
}

// delBlobFunc 删除blob
func delBlobFunc(key string, p *proxyHandler) error {
	v := storage.NewVacuum(context.TODO(), p.driver)
	ref, err := reference.Parse(key)
	if err != nil {
		return fmt.Errorf("Scheduler error returned from OnExpire(%s): %s", key, err)
	}
	var r1 reference.Canonical
	var ok bool
	if r1, ok = ref.(reference.Canonical); !ok {
		return fmt.Errorf("unexpected reference type : %T", ref)
	}
	repo, err := p.App.registry.Repository(context.TODO(), r1)
	if err != nil {
		return err
	}
	blobs := repo.Blobs(context.TODO())
	// Clear the repository reference and descriptor caches
	err = blobs.Delete(context.TODO(), r1.Digest())
	if err != nil {
		return err
	}
	err = v.RemoveBlob(r1.Digest().String())
	if err != nil {
		return err
	}
	return nil
}
