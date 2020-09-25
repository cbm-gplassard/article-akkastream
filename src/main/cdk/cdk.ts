#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { CdkStack } from './lib/cdk-stack';

const app = new cdk.App();
new CdkStack(app, 'ArticleAkkaStream', {
    stackName: "article-akkastream",
    tags: {
        Bloc: "shared",
        App: "antifraude",
        Comp: "article",
        Env: "dev",
        Team: "guardians",
        IsInfraAsCode: "cdk",
    }
});
